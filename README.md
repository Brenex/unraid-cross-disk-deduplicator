# Unraid Cross-Disk Deduplicator — v4.0.0

Find identical files recursively and consolidate them with hardlinks on physical Unraid array disks. Copies on the same disk and on different disks are always eligible. File names and relative paths do not need to match.

**Runs preview changes unless you explicitly supply `--real-run`.** Python 3.9 or newer is required; only the standard library is used.

## Usage

Preview duplicates in a share:

```bash
python3 ./unraid-cross-disk-deduplicator.py /mnt/user/books/
```

The equivalent named argument is:

```bash
python3 ./unraid-cross-disk-deduplicator.py --directory /mnt/user/books/
```

To retain a matching file from a master directory, for example torrent files that you can recheck in your client:

```bash
python3 ./unraid-cross-disk-deduplicator.py \
  --directory /mnt/user/data/media \
  --master-dir /mnt/user/data/torrents
```

After reviewing the preview, add `--real-run` to apply that operation:

```bash
python3 ./unraid-cross-disk-deduplicator.py \
  --directory /mnt/user/data/media \
  --master-dir /mnt/user/data/torrents \
  --real-run
```

Pause downloads, imports, renames, other file writers, and Unraid Mover for the run. Real runs require root. Previews need read access to participating files and configuration, plus write access to the log directory. Reads can update file access times.

## Retained source selection

| Arguments | Retained source |
| --- | --- |
| No `--master-dir` | An existing matching copy on the disk with the most available bytes at startup. |
| One or more `--master-dir` arguments | A matching file under a configured master. Groups without a master match are skipped. |

Selected disks are polled once at startup. Logs and the journal record total, used and available bytes. Ranking uses available bytes rather than free percentage and stays fixed for the run. Ties use disk number, then bytewise path order. When several masters match, this ranking chooses among those masters.

There is no special treatment of names such as `torrent` or `torrents`. To preserve a torrent file as the source, explicitly configure its master directory. Master trees can have any name and are scanned in addition to the input trees. The script does not query torrent clients or tracker membership.

Selection is among disks already containing matching content. The script does not copy data to an otherwise empty disk or move unique files to balance space; hardlinks must share a filesystem.

## Scope and physical paths

`/mnt/user/books/` expands to `/mnt/disk1/books/`, `/mnt/disk2/books/`, and corresponding paths on the other selected mounted array disks. Content is accessed through physical paths, not the merged `/mnt/user` filesystem.

- `--directory` and `--master-dir` are repeatable. Overlapping trees are walked once.
- Logical `/mnt/user/<share>/...` paths expand across selected array disks.
- A physical directory such as `--directory /mnt/disk2/books` is pinned to that disk.
- Positional disk mount roots limit participating disks. Append `/mnt/disk2` for one disk or `/mnt/disk1 /mnt/disk3` for those two disks.
- With no disk arguments, all mounted `/mnt/diskN` array disks are selected. One mounted array disk is sufficient.
- With no input directory, all shares on selected disks are scanned. Otherwise only the input trees are replacement/removal targets.
- A master outside the input trees is a reference candidate, not a removal target. Its retained inode and parent directories can still receive the existing permission repair.
- If an input includes a master tree, other matching master copies within that input scope may be consolidated while preserving their logical paths.
- A requested directory absent on some selected disks is normal. Absence on every selected array disk is an error.

Cache/pool copies, `/mnt/user0`, Unassigned Devices, symlinked paths and nested filesystems are outside scope. Symlinks, empty files and files directly at a disk root are skipped. Copies on unselected disks or pools are not reconciled; inspect physical placement when selecting a subset of disks.

## What changes on disk

The scanner groups by size and calculates SHA-256 for groups with multiple independent inodes. With configured masters, an eligible group must also contain a master candidate. Existing hardlink aliases share hashing work when recorded inode metadata matches.

All hashing finishes before deduplication. Scan/hash errors stop processing before deduplication. Each duplicate receives **one byte-for-byte comparison before linking**, plus inode, metadata and path checks around the operation. No second full content comparison follows hardlink creation.

### Same-disk copies

The retained source keeps its inode. The script creates a `.unraid-dedup-<operation>.tmp` hardlink beside the old copy, syncs and verifies it, then atomically renames it over the old entry. The media pathname stays present throughout replacement. Already-hardlinked paths are left alone.

### Cross-disk copies

For identical files, if disk3 is selected:

```text
Before: /mnt/disk1/books/Author/book.pdf
        /mnt/disk3/books/Other/renamed.pdf
After:  /mnt/disk3/books/Author/book.pdf
        /mnt/disk3/books/Other/renamed.pdf
```

The replacement is created on disk3 at the old copy's relative path, synced and verified before the disk1 copy is removed. Both logical `/mnt/user/books/...` paths remain available if the destination disk participates in that share.

Eligible same-disk copies on the retained disk are handled first to resolve matching destination collisions. Other disks are processed from least to most initial available space. A remaining cross-disk destination occupied by another inode stops the operation; it is never blindly overwritten.

Hardlinks share file data and inode metadata. Modifying a retained file changes its aliases. A replacement uses the retained inode's timestamps and attributes. Direct users of physical `/mnt/diskN/...` paths must account for relocation separately.

## Options

| Option | Behavior |
| --- | --- |
| `--directory PATH`, `--source-dir PATH` | Recursive input directory; repeatable. Positional `/mnt/user/...` inputs are also accepted. |
| `--master-dir PATH` | Directory supplying retained source files; repeatable. Unmatched groups are skipped. |
| `-r`, `--real-run` | Apply changes; requires root. |
| `-d`, `--dry-run` | Explicit preview; optional because preview is already the default. Cannot be combined with `--real-run`. |
| `-e PATH`, `--exclude PATH` | Exclude a path/subtree; repeatable; supports quoted shell-style wildcards. |
| `--min-size BYTES` | Positive minimum size, default 1 byte. Empty files are skipped. |
| `--progress-interval SECONDS` | Heartbeat interval, default 30. Use 10 for more frequent updates or 0 to disable. Must be finite and non-negative. |
| `-v LEVEL`, `--verbose LEVEL` | Console/file level: DEBUG, INFO, WARNING, ERROR or CRITICAL; case-insensitive. Default INFO. |
| `--no-normalize-metadata` | Disable automatic ownership/permission repair and use strict UID/GID/mode/xattr matching. |
| `--allow-metadata-change` | Allow retained inode metadata to replace remaining conflicts. Attributes are not merged. |
| `-p FILE`, `--process-file FILE` | Use an exact inventory of physical file paths, LF-delimited. Cannot be combined with directory/master scopes. |
| `-0`, `--null` | NUL-delimited inventory; requires `-p`. Every record must end with NUL. |
| `--log-dir DIRECTORY` | Log/journal directory; default `logs` beside the script. |
| `--keep-logs N` | Retain N matching `.log` files, default 7. Journals are never automatically removed. |
| `--version`, `-h` / `--help` | Print version or help. |

The obsolete `--mode` and `--include-same-disk` arguments have been removed. Delete them from old commands. Same-disk handling is automatic; only an explicit `--master-dir` constrains source selection. Commands without a master no longer infer torrent priority from path names.

## Exclusions and input lists

Quote wildcard exclusions so the shell passes them unchanged:

```bash
python3 ./unraid-cross-disk-deduplicator.py /mnt/user/data \
  -e '/mnt/disk*/data/torrents/archived/SFF Update */' \
  -e '/mnt/disk*/appdata'
```

Excluding a directory excludes all descendants. `/mnt/user/<share>/...` exclusions map to `/mnt/disk*/<share>/...`; the older `/mnt/user/disk*/...` form is also accepted. Exclusions apply to recursive scans and inventories. An explicitly excluded replacement path is rejected before metadata changes or linking.

File-list paths must be normalized, absolute physical paths within selected disks and under a share directory. Duplicate paths are processed once; missing/unsafe paths abort processing. LF lists preserve whitespace and a final LF is optional. Use NUL-delimited lists for filenames containing newlines:

```bash
find /mnt/disk1/books /mnt/disk2/books -type f -print0 > /tmp/dedup-files.nul
python3 ./unraid-cross-disk-deduplicator.py -p /tmp/dedup-files.nul --null
```

## Metadata and safety

- Pinned directory descriptors reject symlinked ancestors, nested filesystems and unsafe path changes. A host-wide lock coordinates real runs of this script, but cannot stop unrelated writers.
- Content is compared before linking. Device/inode, size, timestamps, ownership, mode and link-count checks detect changes around reads and before mutation.
- A cross-disk destination already referencing the retained inode can finish a partial operation after fresh validation.
- Ownership/permission repair is enabled by default. Participating files become the host's resolved `nobody:users`, mode `666`, including previously read-only or executable regular files. Participating ancestor directories down to the share root become `777`; disk mount roots are excluded. Execute permission is never removed from directories as an intermediate step.
- Repairs cover confirmed duplicate participants and their ancestors. Existing hardlink aliases share inode changes. New directories inherit repaired original directory metadata. Unrelated files' own metadata is not recursively rewritten.
- Previews journal planned metadata repairs without applying them. Real repairs are journaled, verified and synced. Completed repairs remain if a later operation fails.
- Chmod-related POSIX access-ACL changes are projected for previews and checked after repair. Remaining xattr conflicts are skipped unless `--allow-metadata-change` is supplied. With normalization disabled, this override also permits differing ownership/modes.
- An override uses the retained source's attributes, including absence of attributes; it does not merge metadata from the discarded inode. Ownership changes may clear privilege-related attributes under Linux behavior.
- `/boot/config/share.cfg` and `/boot/config/shares/<share>.cfg` are checked for disk inclusion/exclusion and known pool-only settings. Missing global configuration blocks actions; missing per-share configuration uses defaults. Split-level placement and the full allocation engine are not reproduced. A disallowed destination stops the operation.
- Failed preparation retains the original and removes only the script's verified new link when safe. Once removal or rename may have started, the replacement is retained even if interruption, syncing or journaling subsequently fails.
- Validation cannot make pathname replacement/removal atomic with respect to unrelated writers. Pause writers and Mover throughout the run. The journal supports inspection, not automatic rollback or protection against hardware failure.

## Progress, logs and recovery

INFO messages show startup disk usage, scanned files, hashing work, hashing completion and retained sources. A heartbeat runs every 30 seconds by default. It reports phase, current path, elapsed time and byte/file percentage during content reads. Comparisons show both paths and count matched bytes per file rather than combined physical reads. Hashing totals count completed inodes separately from current-file progress.

The monitor reads in-memory counters on a separate thread. While a disk read waits, it can report how long bytes have not advanced. That indicates the monitor is responsive, not that storage is advancing. Scheduling or stalled log output can delay messages. WARNING/ERROR levels suppress INFO progress.

Each validated invocation creates a readable `.log` and durable `.journal.jsonl`. Keep the log directory outside scanned trees, or exclude it, to avoid scanning changing logs. Old logs follow `--keep-logs`; journals remain until manually removed after review.

Journal records include escaped source/original/replacement paths, SHA-256, inode identities and `source_selection`. Initial disk totals and source/original available bytes make selection auditable.

| Event | Meaning |
| --- | --- |
| `DISK_USAGE_SNAPSHOT` | Initial disk space and source-selection policy. |
| `WOULD_DEDUPLICATE`, `WOULD_NORMALIZE_METADATA` | Preview operations; no media mutation. |
| `SKIPPED_METADATA` | Metadata conflict; original retained. |
| `METADATA_PREPARED`, `METADATA_NORMALIZED` | Repair intent and verified result. |
| `PREPARED` | Content compared; replacement intent persisted. |
| `LINK_READY` | Replacement/temporary hardlink created and synced. |
| `VERIFIED` | Metadata checked after linking; final path checks still precede mutation. |
| `ORIGINAL_REMOVED` | Cross-disk original removed and parent synced. |
| `REPLACE_STARTED` | Same-disk rename intent before final checks and rename. |
| `ORIGINAL_REPLACED` | Same-disk pathname replaced with the retained inode and directory synced. |
| `LINK_ROLLED_BACK` | Only the script's new link was removed before original replacement/removal. |
| `RUN_COMPLETE` | Finished invocation and counters. |

After interruption/failure, keep writers paused and inspect actual physical paths and inode identities. Absence of `ORIGINAL_REMOVED` or `ORIGINAL_REPLACED` does not prove the syscall did not occur: a run can stop after a filesystem change and before recording it. Do not blindly replay journal actions or delete paths.

Same-disk records include the exact `temporary_link`. A temporary hardlink may remain after a failed or uncertain rename. Inspect it and the original/replacement identities before cleanup. Empty directories or completed metadata repairs can remain too. A fresh preview rescans and revalidates; completed hardlinks are skipped and validated partial cross-disk operations can be finished by a subsequent real run.

`estimated_reclaimable_bytes` counts allocated blocks only when the discarded inode has one link. Reflinks, snapshots, compression, open handles and accounting can affect recovered space. The script does not configure torrent clients, media managers or Mover.

## Exit status

| Code | Meaning |
| --- | --- |
| `0` | Successful run/preview, including no matches or metadata skips. |
| `1` | Runtime or safety failure; prior completed operations may remain. Inspect the journal. |
| `2` | Invalid arguments or unsupported Python version. |
| `130` | Interrupted by SIGINT/SIGTERM; inspect the journal. |
