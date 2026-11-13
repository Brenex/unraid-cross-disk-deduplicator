#!/usr/bin/env python3
"""Consolidate identical files onto a torrent source's physical Unraid disk.

All filesystem operations use pinned directory descriptors and reject symlinks.
Writers and Mover MUST be paused: a cooperative lock cannot stop other programs.
"""
import argparse
import contextlib
import dataclasses
import datetime
import errno
import fcntl
import fnmatch
import grp
import hashlib
import json
import logging
import math
import os
from pathlib import Path, PurePosixPath
import re
import pwd
import signal
import stat
import struct
import sys
import threading
import time
import uuid
from collections import defaultdict

VERSION = "2.3.0"
CHUNK = 1024 * 1024
DISK_RE = re.compile(r"disk[1-9][0-9]*\Z")
DIR_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
FILE_FLAGS = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC
LOG = logging.getLogger("unraid-dedup")


class SafetyError(Exception):
    pass


class ChangedFile(SafetyError):
    pass


def stamp(s):
    """Ignore atime, which our own reads can update."""
    return (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns,
            s.st_mode, s.st_uid, s.st_gid, s.st_nlink)


def identity(s):
    return s.st_dev, s.st_ino


def content_identity(s):
    return (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_mode, s.st_uid, s.st_gid)


def quoted(path):
    # Escapes control characters and undecodable filename bytes in terminal logs.
    return json.dumps(os.fspath(path), ensure_ascii=True)


def human_bytes(value):
    for unit in ("B", "KiB", "MiB", "GiB", "TiB", "PiB"):
        if value < 1024 or unit == "PiB":
            return "%.1f %s" % (value, unit)
        value /= 1024


class Progress:
    """Report main-thread work, including a read that has not returned yet.

    The monitor only reads in-memory counters and writes ordinary logs. It never
    touches media descriptors or the recovery journal.
    """
    def __init__(self, interval=30):
        self.interval = interval
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.thread = None
        self.state = None

    def start(self, phase, detail="", path=None, other=None, total_bytes=None):
        if not self.interval:
            return
        now = time.monotonic()
        with self.lock:
            self.state = dict(phase=phase, detail=detail, path=path, other=other,
                              total=total_bytes, done=0, started=now, advanced=now)

    def advance(self, amount):
        if not self.interval:
            return
        with self.lock:
            if self.state is not None:
                self.state["done"] += amount
                self.state["advanced"] = time.monotonic()

    def report(self):
        with self.lock:
            state = dict(self.state) if self.state is not None else None
        if state is None:
            return
        now = time.monotonic()
        fields = [state["phase"]]
        if state["detail"]:
            fields.append(state["detail"])
        if state["path"] is not None:
            fields.append("file=" + quoted(state["path"]))
        if state["other"] is not None:
            fields.append("compared_with=" + quoted(state["other"]))
        if state["total"] is not None:
            percent = 100 * state["done"] / state["total"] if state["total"] else 100
            fields.append("file progress=%s/%s (%.1f%%)" % (
                human_bytes(state["done"]), human_bytes(state["total"]), percent))
            idle = now - state["advanced"]
            if idle >= self.interval:
                fields.append("no byte progress for %.0fs; read or I/O may be waiting" % idle)
        fields.append("phase elapsed=%.0fs" % (now - state["started"]))
        LOG.info("Progress: %s", "; ".join(fields))

    def monitor(self):
        while not self.stop.wait(self.interval):
            self.report()

    def __enter__(self):
        self.stop.clear()
        if self.interval:
            self.thread = threading.Thread(target=self.monitor, name="dedup-progress", daemon=True)
            self.thread.start()
        return self

    def close(self):
        self.stop.set()
        if self.thread is not None:
            self.thread.join()
            self.thread = None

    def __exit__(self, *exc):
        self.close()


@dataclasses.dataclass
class Entry:
    root: Path
    relative: str
    snapshot: object
    digest: str = ""

    @property
    def path(self):
        return self.root / self.relative

    @property
    def torrent(self):
        return any(p.lower() in ("torrent", "torrents")
                   for p in PurePosixPath(self.relative).parts[:-1])

    @property
    def share(self):
        return PurePosixPath(self.relative).parts[0]


def parts_of(relative):
    parts = relative.split("/")
    if not parts or any(p in ("", ".", "..") for p in parts):
        raise SafetyError("Unsafe relative path: " + quoted(relative))
    return parts


def xattrs(fd):
    try:
        return {n: os.getxattr(fd, n) for n in os.listxattr(fd)}
    except OSError as exc:
        if exc.errno in (errno.ENOTSUP, errno.EOPNOTSUPP):
            return {}
        raise


def metadata(fd):
    s = os.fstat(fd)
    return s.st_uid, s.st_gid, stat.S_IMODE(s.st_mode), xattrs(fd)



def unraid_owner():
    try:
        return pwd.getpwnam("nobody").pw_uid, grp.getgrnam("users").gr_gid
    except KeyError as exc:
        raise SafetyError("Cannot resolve the Unraid nobody:users accounts") from exc


def normalized_mode(snapshot):
    # Select the final mode by type. Directory traversal is never removed as
    # an intermediate step in file permission repair.
    if stat.S_ISDIR(snapshot.st_mode):
        return 0o777
    if stat.S_ISREG(snapshot.st_mode):
        return 0o666
    raise SafetyError("Permission repair requires a regular file or directory")


def projected_xattrs(attributes, mode):
    """Predict chmod's access-ACL mask changes without changing a dry-run file."""
    projected = dict(attributes)
    acl = attributes.get("system.posix_acl_access")
    if acl is None:
        return projected
    if len(acl) < 4 or (len(acl) - 4) % 8 or struct.unpack_from("<I", acl)[0] != 2:
        raise SafetyError("Unsupported POSIX access ACL encoding")
    entries = [list(struct.unpack_from("<HHI", acl, offset))
               for offset in range(4, len(acl), 8)]
    has_mask = any(entry[0] == 0x10 for entry in entries)
    permissions = {0x01: (mode >> 6) & 7, 0x10: (mode >> 3) & 7, 0x20: mode & 7}
    if not has_mask:
        permissions[0x04] = (mode >> 3) & 7
    for entry in entries:
        if entry[0] in permissions:
            entry[1] = permissions[entry[0]]
    projected["system.posix_acl_access"] = struct.pack("<I", 2) + b"".join(
        struct.pack("<HHI", *entry) for entry in entries)
    return projected


def copy_directory_metadata(source_fd, target_fd):
    s = os.fstat(source_fd)
    target_stat = os.fstat(target_fd)
    if not stat.S_ISDIR(s.st_mode) or not stat.S_ISDIR(target_stat.st_mode):
        raise SafetyError("Directory metadata copying requires directory descriptors")
    directory_mode = stat.S_IMODE(s.st_mode)
    if not directory_mode & 0o111:
        raise SafetyError("Refusing to copy a directory mode with no execute bits; enable metadata normalization to repair it")
    os.fchown(target_fd, s.st_uid, s.st_gid)
    os.fchmod(target_fd, directory_mode)
    desired = xattrs(source_fd)
    # Remove inherited default/access ACLs or attributes absent from the original.
    for name in xattrs(target_fd):
        if name not in desired:
            os.removexattr(target_fd, name)
    for name, value in desired.items():
        os.setxattr(target_fd, name, value)
    os.utime(target_fd, ns=(s.st_atime_ns, s.st_mtime_ns))
    if metadata(source_fd) != metadata(target_fd):
        raise SafetyError("New directory metadata verification failed")
    os.fsync(target_fd)


class PhysicalDisk:
    def __init__(self, path):
        self.path = Path(path)
        self.fd = os.open(self.path, DIR_FLAGS)
        self.snapshot = os.fstat(self.fd)

    def check(self):
        s = os.stat(self.path, follow_symlinks=False)
        if identity(s) != identity(self.snapshot) or not stat.S_ISDIR(s.st_mode):
            raise SafetyError("Disk root changed: " + quoted(self.path))

    def close(self):
        os.close(self.fd)

    @contextlib.contextmanager
    def parent(self, relative, create=False, template=None):
        """Open components without following symlinks; return a pinned parent."""
        parts = parts_of(relative)
        fd = os.dup(self.fd)
        template_fd = os.dup(template.fd) if template is not None else None
        try:
            for component in parts[:-1]:
                next_template = None
                if template_fd is not None:
                    next_template = os.open(component, DIR_FLAGS, dir_fd=template_fd)
                try:
                    try:
                        next_fd = os.open(component, DIR_FLAGS, dir_fd=fd)
                    except FileNotFoundError:
                        if not create or next_template is None:
                            raise
                        os.mkdir(component, mode=0o700, dir_fd=fd)
                        next_fd = os.open(component, DIR_FLAGS, dir_fd=fd)
                        try:
                            copy_directory_metadata(next_template, next_fd)
                            os.fsync(fd)
                        except BaseException:
                            os.close(next_fd)
                            raise
                    if os.fstat(next_fd).st_dev != self.snapshot.st_dev:
                        os.close(next_fd)
                        raise SafetyError("Nested filesystem in destination or source")
                except BaseException:
                    if next_template is not None:
                        os.close(next_template)
                    raise
                os.close(fd)
                fd = next_fd
                if template_fd is not None:
                    os.close(template_fd)
                    template_fd = next_template
            yield fd, parts[-1]
        finally:
            os.close(fd)
            if template_fd is not None:
                os.close(template_fd)

    @contextlib.contextmanager
    def opened(self, entry):
        with self.parent(entry.relative) as (parent_fd, name):
            fd = os.open(name, FILE_FLAGS, dir_fd=parent_fd)
            try:
                s = os.fstat(fd)
                if not stat.S_ISREG(s.st_mode) or s.st_dev != self.snapshot.st_dev:
                    raise SafetyError("Not a regular file on the expected filesystem")
                if stamp(s) != stamp(entry.snapshot):
                    raise ChangedFile("File changed since scan: " + quoted(entry.path))
                yield fd, parent_fd, name
            finally:
                os.close(fd)


def hash_fd(fd, progress=None):
    before = os.fstat(fd)
    os.lseek(fd, 0, os.SEEK_SET)
    digest = hashlib.sha256()
    while True:
        block = os.read(fd, CHUNK)
        if not block:
            break
        digest.update(block)
        if progress is not None:
            progress.advance(len(block))
    if stamp(before) != stamp(os.fstat(fd)):
        raise ChangedFile("File changed while hashing")
    return digest.hexdigest()


def compare_fds(left, right, progress=None):
    before = [os.fstat(fd) for fd in (left, right)]
    if before[0].st_size != before[1].st_size:
        raise ChangedFile("File sizes differ during final comparison")
    for fd in (left, right):
        os.lseek(fd, 0, os.SEEK_SET)
    while True:
        a, b = os.read(left, CHUNK), os.read(right, CHUNK)
        if a != b:
            raise ChangedFile("Contents differ during final comparison")
        if not a:
            break
        if progress is not None:
            # Count matched bytes per file, rather than both physical reads.
            progress.advance(len(a))
    after = [os.fstat(fd) for fd in (left, right)]
    if any(stamp(a) != stamp(b) for a, b in zip(before, after)):
        raise ChangedFile("File changed during final comparison")
    return after


def read_config(path):
    values = {}
    try:
        with open(path, encoding="utf-8") as handle:
            for raw in handle:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                value = value.strip()
                if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
                    value = value[1:-1]
                values[key.strip()] = value
    except FileNotFoundError:
        return None
    return values


class SharePolicy:
    def __init__(self, config_dir=Path("/boot/config")):
        self.directory = Path(config_dir)
        self.global_config = read_config(self.directory / "share.cfg")
        self.cache = {}

    @staticmethod
    def allowed(config, disk, include_key, exclude_key):
        include = set(filter(None, re.split(r"[,\s]+", config.get(include_key, ""))))
        exclude = set(filter(None, re.split(r"[,\s]+", config.get(exclude_key, ""))))
        return (not include or disk in include) and disk not in exclude

    def check(self, share, disk):
        if self.global_config is None:
            raise SafetyError("Cannot verify Unraid share settings: /boot/config/share.cfg is missing")
        if not self.allowed(self.global_config, disk, "shareUserInclude", "shareUserExclude"):
            raise SafetyError("Destination disk is excluded from global user shares")
        if share not in self.cache:
            self.cache[share] = read_config(self.directory / "shares" / (share + ".cfg")) or {}
        config = self.cache[share]
        if not self.allowed(config, disk, "shareInclude", "shareExclude"):
            raise SafetyError("Destination disk is excluded from this user share")
        if config.get("shareUseCache", "").lower() == "only":
            raise SafetyError("Destination user share is configured for pool-only storage")
        primary = config.get("sharePrimaryStorage")
        if primary and primary != "array" and config.get("shareSecondaryStorage", "") != "array":
            raise SafetyError("Destination user share is configured for pool-only storage")


class Journal:
    def __init__(self, path):
        self.handle = open(path, "x", encoding="utf-8")
        os.chmod(path, 0o600)
        # Persist the journal's directory entry before any media mutations.
        fd = os.open(Path(path).parent, DIR_FLAGS)
        try:
            self.handle.flush()
            os.fsync(self.handle.fileno())
            os.fsync(fd)
        finally:
            os.close(fd)

    def write(self, event, **fields):
        record = {"time": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                  "event": event, **fields}
        self.handle.write(json.dumps(record, ensure_ascii=True, sort_keys=True) + "\n")
        self.handle.flush()
        os.fsync(self.handle.fileno())

    def close(self):
        self.handle.close()


class Engine:
    def __init__(self, args, roots, journal=None, policy=None, owner=None):
        self.args = args
        self.normalization_owner = (owner if owner is not None else unraid_owner()) if args.normalize_metadata else None
        self.metadata_seen = set()
        self.roots = {str(root): PhysicalDisk(root) for root in roots}
        self.journal = journal
        self.policy = policy if policy is not None else SharePolicy()
        self.counts = defaultdict(int)
        self.errors = 0
        self.exclusions = self.prepare_exclusions(args.exclude)
        self.inventory = []
        self.progress = Progress(getattr(args, "progress_interval", 30))
        self.operation_detail = ""

    def close(self):
        self.progress.close()
        for disk in self.roots.values():
            disk.close()

    @staticmethod
    def prepare_exclusions(patterns):
        result = []
        for value in patterns:
            value = value.rstrip("/")
            if value.startswith("/mnt/user/disk*/"):
                value = "/mnt/disk*/" + value[len("/mnt/user/disk*/"):]
            elif value.startswith("/mnt/user/"):
                value = "/mnt/disk*/" + value[len("/mnt/user/"):]
            if not value.startswith("/") or ".." in value.split("/"):
                raise SafetyError("Exclusions must be absolute paths without '..'")
            result.append(value)
        return result

    def excluded(self, path):
        # Match each ancestor: excluding a directory also excludes its contents.
        current = os.fspath(path)
        while current != "/":
            if any(fnmatch.fnmatchcase(current, pattern) for pattern in self.exclusions):
                return True
            current = os.path.dirname(current)
        return False

    def error(self, message, exc):
        self.errors += 1
        LOG.error("%s: %s", message, ascii(str(exc)))

    def scan(self):
        if self.args.process_file:
            self.scan_list()
        else:
            for disk in self.roots.values():
                LOG.info("Scanning %s", quoted(disk.path))
                self.walk(disk, "")
        # Reject duplicate input paths; order is independent of filesystem iteration.
        self.progress.start("Sorting scanned paths", detail="inventory files=%s" % len(self.inventory))
        unique = {(str(e.root), e.relative): e for e in self.inventory}
        self.inventory = sorted(unique.values(), key=lambda e: os.fsencode(e.path))
        self.counts["files_scanned"] = len(self.inventory)
        if self.errors:
            raise SafetyError("Scan incomplete; refusing to process a partial inventory")
        LOG.info("Scan complete: %s files across %s disks; grouping candidates by size",
                 len(self.inventory), len(self.roots))

    def walk(self, disk, relative):
        if self.excluded(disk.path / relative):
            return
        self.progress.start("Scanning directories", path=disk.path / relative,
                            detail="inventory files=%s" % len(self.inventory))
        # Open the directory through its anchored parent, including the disk root.
        try:
            with disk.parent(relative + "/.scan" if relative else ".scan") as (fd, _):
                with os.scandir(fd) as entries:
                    for item in entries:
                        rel = relative + "/" + item.name if relative else item.name
                        path = disk.path / rel
                        if self.excluded(path):
                            continue
                        s = item.stat(follow_symlinks=False)
                        if s.st_dev != disk.snapshot.st_dev:
                            self.counts["nested_mounts_skipped"] += 1
                            continue
                        if stat.S_ISDIR(s.st_mode):
                            self.walk(disk, rel)
                        elif stat.S_ISREG(s.st_mode) and s.st_size >= self.args.min_size:
                            # A user-share path needs a top-level share directory.
                            if "/" in rel:
                                self.inventory.append(Entry(disk.path, rel, s))
                        elif stat.S_ISLNK(s.st_mode):
                            self.counts["symlinks_skipped"] += 1
        except OSError as exc:
            self.error("Scan failed for " + quoted(disk.path / relative), exc)

    def scan_list(self):
        raw = Path(self.args.process_file).read_bytes()
        if self.args.null:
            if raw and not raw.endswith(b"\0"):
                raise SafetyError("NUL-delimited input must end with NUL")
            records = raw.split(b"\0")
        else:
            # Only LF is a delimiter; do not strip legal filename whitespace.
            records = raw.split(b"\n")
        for record in records:
            if not record:
                continue
            path = os.fsdecode(record)
            self.progress.start("Scanning input list", path=path,
                                detail="inventory files=%s" % len(self.inventory))
            try:
                if not os.path.isabs(path) or os.path.normpath(path) != path:
                    raise SafetyError("Input paths must be absolute and normalized")
                matched = False
                for disk in self.roots.values():
                    prefix = str(disk.path) + "/"
                    if path.startswith(prefix):
                        matched = True
                        if self.excluded(path):
                            break
                        relative = path[len(prefix):]
                        if "/" not in relative:
                            raise SafetyError("Input file is outside a user-share directory")
                        with disk.parent(relative) as (fd, name):
                            s = os.stat(name, dir_fd=fd, follow_symlinks=False)
                        if not stat.S_ISREG(s.st_mode) or s.st_dev != disk.snapshot.st_dev:
                            raise SafetyError("Input is not a regular file on its physical disk")
                        if s.st_size >= self.args.min_size:
                            self.inventory.append(Entry(disk.path, relative, s))
                        break
                if not matched:
                    raise SafetyError("Input path is outside the selected physical disks")
            except (OSError, SafetyError) as exc:
                self.error("Invalid input path " + quoted(path), exc)

    def groups(self):
        self.progress.start("Grouping files by size", detail="inventory files=%s" % len(self.inventory))
        by_size = defaultdict(list)
        for entry in self.inventory:
            by_size[entry.snapshot.st_size].append(entry)
        hashed = {}
        groups = []
        self.progress.start("Selecting cross-disk hashing candidates")
        sizes = [size for size in sorted(by_size)
                 if len({str(e.root) for e in by_size[size]}) >= 2
                 and any(e.torrent for e in by_size[size])]
        total_inodes = total_bytes = total_paths = 0
        for size in sizes:
            count = len({stamp(e.snapshot) for e in by_size[size]})
            total_inodes += count
            total_bytes += size * count
            total_paths += len(by_size[size])
        LOG.info("Hashing candidates: %s paths; %s unique inodes; %s across %s size groups; "
                 "progress interval=%ss (0 disables heartbeat)",
                 total_paths, total_inodes, human_bytes(total_bytes), len(sizes), self.progress.interval)
        started = time.monotonic()
        for size in sizes:
            entries = by_size[size]
            by_hash = defaultdict(list)
            for entry in entries:
                try:
                    detail = "completed hashes=%s/%s; completed hash bytes=%s/%s" % (
                        self.counts["inodes_hashed"], total_inodes,
                        human_bytes(self.counts["bytes_hashed"]), human_bytes(total_bytes))
                    self.progress.start("Hashing SHA-256", path=entry.path,
                                        total_bytes=size, detail=detail)
                    with self.roots[str(entry.root)].opened(entry) as (fd, _, _):
                        key = stamp(entry.snapshot)
                        if key not in hashed:
                            hashed[key] = hash_fd(fd, progress=self.progress)
                            self.counts["inodes_hashed"] += 1
                            self.counts["bytes_hashed"] += size
                        entry.digest = hashed[key]
                    by_hash[entry.digest].append(entry)
                except (OSError, SafetyError) as exc:
                    self.error("Hash failed for " + quoted(entry.path), exc)
            self.progress.start("Grouping matching hashes", detail="size=%s" % human_bytes(size))
            for digest in sorted(by_hash):
                group = by_hash[digest]
                if len({str(e.root) for e in group}) > 1 and any(e.torrent for e in group):
                    groups.append(group)
        # Complete hashing before any mutation; an I/O error aborts the whole plan.
        if self.errors:
            raise SafetyError("Hashing incomplete; no deduplication was performed")
        LOG.info("Hashing complete: %s inodes; %s; %.1fs; %s cross-disk duplicate groups",
                 self.counts["inodes_hashed"], human_bytes(self.counts["bytes_hashed"]),
                 time.monotonic() - started, len(groups))
        return groups

    def record(self, event, **fields):
        if self.journal:
            self.journal.write(event, **fields)

    def normalize_fd(self, fd, path, operation):
        before = os.fstat(fd)
        mode = normalized_mode(before)
        uid, gid = self.normalization_owner
        if (before.st_uid, before.st_gid, stat.S_IMODE(before.st_mode)) == (uid, gid, mode):
            return before
        key = (*identity(before), uid, gid, mode)
        if not self.args.real_run and key in self.metadata_seen:
            return before
        details = dict(operation=operation, path=str(path), inode=list(identity(before)),
                       before=dict(uid=before.st_uid, gid=before.st_gid,
                                   mode=format(stat.S_IMODE(before.st_mode), "04o")),
                       after=dict(uid=uid, gid=gid, mode=format(mode, "04o")))
        if not self.args.real_run:
            self.record("WOULD_NORMALIZE_METADATA", **details)
            LOG.info("WOULD normalize %s: uid=%s gid=%s mode=%04o -> nobody:users (%s:%s) mode=%04o",
                     quoted(path), before.st_uid, before.st_gid, stat.S_IMODE(before.st_mode), uid, gid, mode)
            self.counts["metadata_normalizations_planned"] += 1
            self.metadata_seen.add(key)
            return before
        self.record("METADATA_PREPARED", **details)
        # chown can clear special permission bits. Set the final mode afterward.
        if (before.st_uid, before.st_gid) != (uid, gid):
            os.fchown(fd, uid, gid)
        os.fchmod(fd, mode)
        os.fsync(fd)
        after = os.fstat(fd)
        if (after.st_uid, after.st_gid, stat.S_IMODE(after.st_mode)) != (uid, gid, mode):
            raise SafetyError("Metadata normalization did not produce the requested ownership/mode")
        if (identity(before), before.st_size, before.st_mtime_ns, before.st_nlink) != (
                identity(after), after.st_size, after.st_mtime_ns, after.st_nlink):
            raise ChangedFile("File changed during metadata normalization")
        self.record("METADATA_NORMALIZED", **details)
        LOG.info("Normalized %s to nobody:users (%s:%s), mode=%04o", quoted(path), uid, gid, mode)
        self.counts["metadata_normalizations_completed"] += 1
        for entry in self.inventory:
            if identity(entry.snapshot) == identity(before):
                entry.snapshot = after
        return after

    def normalize_parents(self, disk, relative, operation, allow_missing=False):
        parts = parts_of(relative)
        for depth in range(1, len(parts)):
            parent_relative = "/".join(parts[:depth])
            try:
                with disk.parent(parent_relative + "/.dedup-permissions") as (fd, _):
                    self.normalize_fd(fd, disk.path / parent_relative, operation)
            except FileNotFoundError:
                if allow_missing:
                    break
                raise

    def refresh_source_aliases(self, source):
        # Creating a hardlink changes ctime/nlink for every alias of its inode.
        disk = self.roots[str(source.root)]
        with disk.parent(source.relative) as (fd, name):
            now = os.stat(name, dir_fd=fd, follow_symlinks=False)
        if content_identity(now) != content_identity(source.snapshot):
            raise ChangedFile("Torrent source changed after an operation")
        for entry in self.inventory:
            if identity(entry.snapshot) == identity(source.snapshot):
                entry.snapshot = now

    def run(self):
        with self.progress:
            return self._run()

    def _run(self):
        self.inventory = []
        self.counts.clear()
        self.errors = 0
        self.metadata_seen.clear()
        self.scan()
        groups = self.groups()
        self.counts["duplicate_groups"] = len(groups)
        for group_number, group in enumerate(groups, 1):
            self.progress.start("Selecting retained torrent source",
                                detail="duplicate group=%s/%s" % (group_number, len(groups)))
            source = min((e for e in group if e.torrent), key=lambda e: os.fsencode(e.path))
            for duplicate in sorted(group, key=lambda e: os.fsencode(e.path)):
                if duplicate.root == source.root:
                    continue
                try:
                    self.operation_detail = "duplicate group=%s/%s; planned=%s; completed=%s; metadata skips=%s" % (
                        group_number, len(groups), self.counts["planned"], self.counts["completed"],
                        self.counts["metadata_conflicts_skipped"])
                    self.progress.start("Checking duplicate paths and share policy", path=duplicate.path,
                                        other=source.path, detail=self.operation_detail)
                    self.deduplicate(source, duplicate)
                    if self.args.real_run:
                        self.progress.start("Refreshing retained inode aliases", path=source.path,
                                            detail=self.operation_detail)
                        self.refresh_source_aliases(source)
                except (OSError, SafetyError) as exc:
                    self.error("Operation failed; original retained unless journal records its removal", exc)
                    # Fail closed: stop after the first mutation/preflight failure.
                    raise SafetyError("Stopped; inspect the log and journal before retrying") from exc
        return dict(self.counts)

    def deduplicate(self, source, duplicate):
        source_disk, old_disk = self.roots[str(source.root)], self.roots[str(duplicate.root)]
        source_disk.check()
        old_disk.check()
        self.policy.check(duplicate.share, source.root.name)
        target = source.root / duplicate.relative
        operation = uuid.uuid4().hex
        fields = dict(operation=operation, source=str(source.path), original=str(duplicate.path),
                      target=str(target), sha256=source.digest,
                      source_inode=list(identity(source.snapshot)),
                      original_inode=list(identity(duplicate.snapshot)))
        created = False
        target_identity = None
        old_removed = False
        with source_disk.opened(source) as (source_fd, source_parent, source_name):
            with old_disk.opened(duplicate) as (old_fd, old_parent, old_name):
                self.progress.start("Comparing duplicate contents", path=duplicate.path, other=source.path,
                                    total_bytes=duplicate.snapshot.st_size, detail=self.operation_detail)
                source_before, old_before = compare_fds(source_fd, old_fd, progress=self.progress)
                self.progress.start("Checking metadata and permissions", path=duplicate.path,
                                    other=source.path, detail=self.operation_detail)
                if stamp(source_before) != stamp(source.snapshot) or stamp(old_before) != stamp(duplicate.snapshot):
                    raise ChangedFile("File changed after opening and before final comparison")
                # Detect collisions before making permission changes to any participant.
                self.validate_dry_target(source_disk, duplicate.relative, identity(source_before))
                source_metadata, old_metadata = metadata(source_fd), metadata(old_fd)
                if self.args.normalize_metadata:
                    source_attributes = projected_xattrs(source_metadata[3], normalized_mode(source_before))
                    old_attributes = projected_xattrs(old_metadata[3], normalized_mode(old_before))
                    conflict = source_attributes != old_attributes
                    reason = "extended-attribute"
                else:
                    conflict = source_metadata != old_metadata
                    reason = "metadata"
                if conflict and not self.args.allow_metadata_change:
                    self.counts["metadata_conflicts_skipped"] += 1
                    LOG.warning("Skipping %s mismatch: %s", reason, quoted(duplicate.path))
                    self.record("SKIPPED_METADATA", reason=reason, **fields)
                    return
                if conflict:
                    LOG.warning("Using retained source attributes for %s", quoted(target))
                if self.args.normalize_metadata:
                    self.normalize_parents(source_disk, source.relative, operation)
                    self.normalize_parents(old_disk, duplicate.relative, operation)
                    self.normalize_parents(source_disk, duplicate.relative, operation, allow_missing=True)
                    if stamp(os.fstat(source_fd)) != stamp(source_before) or stamp(os.fstat(old_fd)) != stamp(old_before):
                        raise ChangedFile("File changed before metadata normalization")
                    source_before = self.normalize_fd(source_fd, source.path, operation)
                    old_before = self.normalize_fd(old_fd, duplicate.path, operation)
                    if self.args.real_run and not self.args.allow_metadata_change and xattrs(source_fd) != xattrs(old_fd):
                        raise SafetyError("Extended attributes differ after metadata normalization")
                self.record("PREPARED" if self.args.real_run else "WOULD_DEDUPLICATE", **fields)
                if not self.args.real_run:
                    self.counts["planned"] += 1
                    if duplicate.snapshot.st_nlink == 1:
                        self.counts["estimated_reclaimable_bytes"] += duplicate.snapshot.st_blocks * 512
                    LOG.info("WOULD deduplicate: original=%s; retained_source=%s; "
                             "replacement_hardlink=%s; then remove original=%s",
                             quoted(duplicate.path), quoted(source.path),
                             quoted(target), quoted(duplicate.path))
                    return
                self.progress.start("Creating and syncing replacement hardlink", path=target,
                                    detail=self.operation_detail)
                with source_disk.parent(duplicate.relative, create=True, template=old_disk) as (target_parent, target_name):
                    try:
                        try:
                            target_stat = os.stat(target_name, dir_fd=target_parent, follow_symlinks=False)
                        except FileNotFoundError:
                            target_stat = None
                        if target_stat is not None:
                            if not stat.S_ISREG(target_stat.st_mode) or identity(target_stat) != identity(os.fstat(source_fd)):
                                raise SafetyError("Destination collision: " + quoted(target))
                            target_identity = identity(target_stat)
                        else:
                            os.link(source_name, target_name, src_dir_fd=source_parent,
                                    dst_dir_fd=target_parent, follow_symlinks=False)
                            created = True
                            target_stat = os.stat(target_name, dir_fd=target_parent, follow_symlinks=False)
                            target_identity = identity(target_stat)
                            if not stat.S_ISREG(target_stat.st_mode) or target_identity != identity(os.fstat(source_fd)):
                                raise ChangedFile("Source pathname changed while creating the hardlink")
                        os.fsync(source_fd)
                        os.fsync(target_parent)
                        self.record("LINK_READY", **fields)
                        self.progress.start("Verifying replacement contents", path=duplicate.path,
                                            other=source.path, total_bytes=duplicate.snapshot.st_size,
                                            detail=self.operation_detail)
                        source_now, old_now = compare_fds(source_fd, old_fd, progress=self.progress)
                        if content_identity(source_now) != content_identity(source_before) or stamp(old_now) != stamp(old_before):
                            raise ChangedFile("File changed during replacement creation")
                        self.record("VERIFIED", **fields)
                        self.progress.start("Verifying paths before original removal", path=duplicate.path,
                                            detail=self.operation_detail)
                        source_disk.check()
                        old_disk.check()
                        # Ensure the pinned parent directories still have their expected paths.
                        with source_disk.parent(duplicate.relative) as (fresh, _):
                            if identity(os.fstat(fresh)) != identity(os.fstat(target_parent)):
                                raise ChangedFile("Destination parent was moved")
                        with source_disk.parent(source.relative) as (fresh, _):
                            if identity(os.fstat(fresh)) != identity(os.fstat(source_parent)):
                                raise ChangedFile("Source parent was moved")
                        with old_disk.parent(duplicate.relative) as (fresh, _):
                            if identity(os.fstat(fresh)) != identity(os.fstat(old_parent)):
                                raise ChangedFile("Original parent was moved")
                        if stamp(os.stat(source_name, dir_fd=source_parent, follow_symlinks=False)) != stamp(source_now):
                            raise ChangedFile("Source changed before unlink")
                        if stamp(os.stat(old_name, dir_fd=old_parent, follow_symlinks=False)) != stamp(old_now):
                            raise ChangedFile("Original changed before unlink")
                        current_target = os.stat(target_name, dir_fd=target_parent, follow_symlinks=False)
                        if not stat.S_ISREG(current_target.st_mode) or identity(current_target) != identity(source_now):
                            raise ChangedFile("Destination changed before unlink")
                        # Lasting replacement is verified and its directory entry synced.
                        # Set the conservative flag BEFORE unlink so an asynchronous
                        # signal can never trigger rollback after a successful unlink.
                        self.progress.start("Removing verified original and syncing directory", path=duplicate.path,
                                            detail=self.operation_detail)
                        old_removed = True
                        os.unlink(old_name, dir_fd=old_parent)
                        os.fsync(old_parent)
                        old_identity = identity(duplicate.snapshot)
                        old_after = os.fstat(old_fd)
                        for alias in self.inventory:
                            if identity(alias.snapshot) == old_identity:
                                alias.snapshot = old_after
                        self.record("ORIGINAL_REMOVED", **fields)
                        self.counts["completed"] += 1
                        if old_now.st_nlink == 1:
                            self.counts["estimated_reclaimable_bytes"] += old_now.st_blocks * 512
                        LOG.info("DEDUPLICATED: original_removed=%s; retained_source=%s; "
                                 "replacement_hardlink=%s", quoted(duplicate.path),
                                 quoted(source.path), quoted(target))
                    except BaseException:
                        # Roll back only our link, only before original removal, only if unchanged.
                        # After unlink, ALWAYS retain the replacement even if logging/fsync fails.
                        if created and not old_removed and target_identity is not None:
                            try:
                                now = os.stat(target_name, dir_fd=target_parent, follow_symlinks=False)
                                original_now = os.stat(old_name, dir_fd=old_parent, follow_symlinks=False)
                                if identity(now) == target_identity and identity(original_now) == identity(os.fstat(old_fd)):
                                    os.unlink(target_name, dir_fd=target_parent)
                                    os.fsync(target_parent)
                                    self.record("LINK_ROLLED_BACK", **fields)
                            except OSError as exc:
                                LOG.error("Rollback failed; original retained: %s", ascii(str(exc)))
                        raise

    @staticmethod
    def validate_dry_target(disk, relative, source_identity):
        parts = parts_of(relative)
        fd = os.dup(disk.fd)
        try:
            for component in parts[:-1]:
                try:
                    next_fd = os.open(component, DIR_FLAGS, dir_fd=fd)
                except FileNotFoundError:
                    return
                if os.fstat(next_fd).st_dev != disk.snapshot.st_dev:
                    os.close(next_fd)
                    raise SafetyError("Nested filesystem in destination")
                os.close(fd)
                fd = next_fd
            try:
                existing = os.stat(parts[-1], dir_fd=fd, follow_symlinks=False)
            except FileNotFoundError:
                return
            if not stat.S_ISREG(existing.st_mode) or identity(existing) != source_identity:
                raise SafetyError("Destination collision: " + quoted(disk.path / relative))
        finally:
            os.close(fd)


def arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("-r", "--real-run", action="store_true", help="apply verified deduplication")
    mode.add_argument("-d", "--dry-run", action="store_true", help="preview only (default)")
    parser.add_argument("-p", "--process-file", help="absolute disk paths, one per LF-delimited line")
    parser.add_argument("-0", "--null", action="store_true", help="input list is NUL-delimited (requires -p)")
    parser.add_argument("-e", "--exclude", action="append", default=[], metavar="PATH", help="exclude a path/subtree; repeatable; quote wildcards")
    parser.add_argument("-v", "--verbose", type=str.upper, choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"), default="INFO")
    parser.add_argument("--min-size", type=int, default=1, metavar="BYTES", help="minimum file size (default: 1; empty files save no data)")
    parser.add_argument("--progress-interval", type=float, default=30, metavar="SECONDS",
                        help="log current phase/file and byte progress every N seconds (default: 30; 0 disables)")
    parser.add_argument("--no-normalize-metadata", dest="normalize_metadata", action="store_false", default=True,
                        help="disable nobody:users ownership and Unraid permission repair")
    parser.add_argument("--allow-metadata-change", action="store_true",
                        help="allow retained torrent metadata to replace remaining metadata/xattr conflicts")
    parser.add_argument("--log-dir", default=str(Path(__file__).parent.resolve() / "logs"))
    parser.add_argument("--keep-logs", type=int, default=7, help="retain this many .log files; journals are never pruned")
    parser.add_argument("--version", action="version", version=VERSION)
    parser.add_argument("disks", nargs="*", help="physical /mnt/diskN mount roots (default: all mounted array disks)")
    args = parser.parse_args(argv)
    if args.null and not args.process_file:
        parser.error("--null requires --process-file")
    if args.min_size < 1 or args.keep_logs < 1:
        parser.error("--min-size and --keep-logs must be positive")
    if not math.isfinite(args.progress_interval) or args.progress_interval < 0:
        parser.error("--progress-interval must be a finite non-negative number")
    return args


def validate_roots(paths):
    if not paths:
        paths = [str(p) for p in Path("/mnt").glob("disk*") if DISK_RE.fullmatch(p.name) and os.path.ismount(p)]
    roots, devices = [], set()
    for raw in paths:
        path = Path(raw)
        if str(path) != raw.rstrip("/") or path.parent != Path("/mnt") or not DISK_RE.fullmatch(path.name):
            raise SafetyError("Only physical /mnt/diskN mount roots are supported: " + quoted(raw))
        if path.is_symlink() or path.resolve() != path or not os.path.ismount(path):
            raise SafetyError("Disk path is not a direct mounted filesystem: " + quoted(raw))
        dev = path.stat().st_dev
        if dev in devices:
            raise SafetyError("Repeated filesystem or disk mount: " + quoted(raw))
        devices.add(dev)
        roots.append(path)
    if len(roots) < 2:
        raise SafetyError("Select at least two mounted physical array disks")
    return sorted(roots, key=lambda p: int(p.name[4:]))


def setup_logging(args):
    directory = Path(args.log_dir).absolute()
    directory.mkdir(parents=True, exist_ok=True)
    if directory.is_symlink():
        raise SafetyError("Log directory must not be a symlink")
    tag = datetime.datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + str(os.getpid())
    path = directory / ("deduplicator_" + tag + ".log")
    handler = logging.FileHandler(path, mode="x", encoding="utf-8")
    os.chmod(path, 0o600)
    console = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    for output in (handler, console):
        output.setFormatter(formatter)
        output.setLevel(args.verbose)
        LOG.addHandler(output)
    LOG.setLevel(logging.DEBUG)
    old = sorted((p for p in directory.glob("deduplicator_*.log") if p.is_file() and not p.is_symlink()),
                 key=lambda p: p.stat().st_mtime_ns, reverse=True)
    for candidate in old[args.keep_logs:]:
        candidate.unlink()
    journal = Journal(directory / ("deduplicator_" + tag + ".journal.jsonl"))
    return journal


@contextlib.contextmanager
def run_lock(real_run):
    # Fixed host-wide lock, independent of selected roots and --log-dir.
    if not real_run:
        yield
        return
    os.makedirs("/run/lock", mode=0o755, exist_ok=True)
    fd = os.open("/run/lock/unraid-cross-disk-deduplicator.lock",
                 os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise SafetyError("Lock path is not a regular file")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise SafetyError("Another real deduplication run is active") from exc
        yield
    finally:
        os.close(fd)


def interrupted(signum, frame):
    raise KeyboardInterrupt("received signal " + str(signum))


def main(argv=None):
    if sys.version_info < (3, 9):
        print("ERROR: Python 3.9 or newer is required", file=sys.stderr)
        return 2
    args = arguments(argv)
    journal, engine = None, None
    os.umask(0o077)
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    try:
        if args.real_run and os.geteuid() != 0:
            raise SafetyError("Real runs require root privileges")
        roots = validate_roots(args.disks)
        journal = setup_logging(args)
        LOG.info("Version %s; mode=%s; physical disks=%s", VERSION,
                 "REAL" if args.real_run else "DRY RUN", ", ".join(map(str, roots)))
        if args.real_run:
            LOG.warning("Writers and Mover must already be paused; the lock only coordinates this script")
        with run_lock(args.real_run):
            engine = Engine(args, roots, journal=journal)
            counts = engine.run()
            journal.write("RUN_COMPLETE", real_run=args.real_run, counts=counts)
            LOG.info("Summary: %s", json.dumps(counts, sort_keys=True))
        return 0
    except KeyboardInterrupt:
        LOG.error("Interrupted; inspect the journal before retrying")
        return 130
    except (OSError, SafetyError) as exc:
        if LOG.handlers:
            LOG.error("Stopped: %s", ascii(str(exc)))
        else:
            print("ERROR: " + ascii(str(exc)), file=sys.stderr)
        return 1
    finally:
        if engine:
            engine.close()
        if journal:
            journal.close()


if __name__ == "__main__":
    sys.exit(main())
