# Unraid Cross-Disk Deduplicator — v2.3.1

Consolidate identical files from different **physical array disks** onto a chosen torrent file's disk. Keep the library's relative path and replace its independent copy with a same-filesystem hardlink. Default operation is **dry-run**.

The script requires **Python 3.9+** and uses only the standard library.

## What changes on disk

Before:

```text
Torrent: /mnt/disk1/data/torrents/Release/original.mkv
Library: /mnt/disk2/data/media/Movies/Title/renamed.mkv
```

After a successful real run:

```text
Torrent: /mnt/disk1/data/torrents/Release/original.mkv
Library: /mnt/disk1/data/media/Movies/Title/renamed.mkv
```

The two remaining paths share one inode. The torrent source and old media copy are compared byte-for-byte once, before creating the replacement hardlink. The second disk's library entry is removed only after the replacement is created and synced, its inode/path identity is checked, and file metadata is rechecked for changes. No second content comparison is performed. The logical library path remains `/mnt/user/data/media/Movies/Title/renamed.mkv`, provided the destination disk participates in that user share. Consumers using `/mnt/disk2/...` directly must have their paths updated separately.

This does not create hardlinks across filesystems. It creates the library hardlink on the torrent file's physical filesystem.

## Before running

1. Keep a verified backup. Hardlinking changes which inode backs the library entry.
2. **Pause torrent downloads, imports, renames, library writers and Unraid Mover for the entire run.** An idle or seeding-only torrent is suitable only if no program writes, replaces or relocates its files. A host-wide lock prevents another real run of this script; it cannot lock out unrelated programs.
3. Confirm `python3 --version` reports 3.9 or newer. The script does not install packages.
4. Use the physical `/mnt/diskN` mount roots as positional arguments. `/mnt/user`, `/mnt/user0`, cache/pool mounts, Unassigned Devices, symlinked roots and subdirectories are rejected.
5. Run a dry-run first, inspect its log and journal, then invoke `--real-run`. Real runs require root. Dry runs need permission to read the selected files and Unraid configuration and write the log directory.

The scanner skips symlinks, empty files, files directly at a disk's root and nested filesystems. It requires at least two mounted array filesystems. ZFS datasets or Btrfs subvolumes that present separate device IDs are skipped rather than assumed to support hardlinks.

## Installation and first dry-run

Save the script in your preferred scripts directory, then:

```bash
python3 --version
python3 ./unraid-cross-disk-deduplicator.py --dry-run /mnt/disk1 /mnt/disk2
```

With no disk arguments, the script selects all mounted `/mnt/diskN` array disks, excluding `/mnt/disks` and pools.

Logs default to a `logs` directory beside the script. You can place them on persistent storage with `--log-dir`; a location outside the scanned shares avoids scanning past journals and logs.

Example with multiple exclusions:

```bash
python3 ./unraid-cross-disk-deduplicator.py --dry-run \
  -e '/mnt/disk*/appdata' \
  -e '/mnt/disk*/system' \
  -e '/mnt/disk*/domains' \
  /mnt/disk1 /mnt/disk2
```

Quote wildcard exclusions so the shell passes each pattern unchanged. Excluding a directory excludes all descendants. `/mnt/user/<share>/...` exclusions are mapped to `/mnt/disk*/<share>/...`; the legacy `/mnt/user/disk*/...` pattern is also accepted. Exclusions apply to both scans and input lists.

After reviewing the dry-run, and with writers still paused, use the same arguments with `--real-run`:

```bash
python3 ./unraid-cross-disk-deduplicator.py --real-run \
  -e '/mnt/disk*/appdata' \
  -e '/mnt/disk*/system' \
  -e '/mnt/disk*/domains' \
  /mnt/disk1 /mnt/disk2
```

## Options

| Option | Behavior |
| --- | --- |
| `-d`, `--dry-run` | Preview only; default. Writes logs/journal, but does not change media or create media directories. Reads may update access times. |
| `-r`, `--real-run` | Apply verified operations; requires root. Cannot be combined with `-d`. |
| `-p FILE`, `--process-file FILE` | Read absolute physical disk paths from an LF-delimited file. A final LF is optional. |
| `-0`, `--null` | Read a NUL-delimited input list; requires `-p`. Each record, including the last, must end with NUL. |
| `-e PATH`, `--exclude PATH` | Exclude a path or subtree; repeatable; supports quoted shell-style wildcards. |
| `-v LEVEL`, `--verbose LEVEL` | Console/file level: DEBUG, INFO, WARNING, ERROR or CRITICAL; case-insensitive. Default INFO. |
| `--min-size BYTES` | Minimum size to consider; positive integer; default 1. For large media only, use e.g. `--min-size 104857600` for 100 MiB. |
| `--progress-interval SECONDS` | Log current phase, file and byte progress every N seconds; default 30. Use 10 for more frequent updates or 0 to disable periodic messages. Must be finite and non-negative. |
| `--no-normalize-metadata` | Disable automatic Unraid ownership/permission repair and use strict metadata matching. |
| `--allow-metadata-change` | Allow the retained source's attributes to replace remaining extended-attribute conflicts. With normalization disabled, also allow ownership/mode conflicts. |
| `--log-dir DIRECTORY` | Directory for logs and durable JSONL journals. |
| `--keep-logs N` | Retain N matching `.log` files, default 7. Journals are never automatically removed. |
| `--version` | Print version. |
| `-h`, `--help` | Print usage. |

Input paths must be normalized, absolute, within the selected disks and under a user-share directory. Duplicate input paths are processed once. Missing, unsafe or inaccessible input files abort processing rather than silently creating an incomplete plan. File lists do not override disk selection: provide the corresponding mount roots when limiting the selected disks.

For names containing newlines, use a NUL-delimited list:

```bash
find /mnt/disk1/data /mnt/disk2/data -type f -print0 > /tmp/dedup-files.nul
python3 ./unraid-cross-disk-deduplicator.py --dry-run \
  --process-file /tmp/dedup-files.nul --null /mnt/disk1 /mnt/disk2
```

Do not edit the NUL-delimited file with a line-oriented text editor. LF lists preserve filename whitespace exactly; CRLF conversion changes the paths.

## How duplicate detection works

1. Scan the selected physical filesystems without following symlinks. A scan error stops processing before any deduplication.
2. Group files by size. Hash only size groups that span disks and contain a torrent-priority candidate. Matching basenames are **not** required.
3. Calculate SHA-256 using streamed reads. Existing hardlink aliases share a checksum calculation when their inode and recorded metadata match. A hashing error aborts the whole plan before mutations.
4. Group matching content and choose a source deterministically: the bytewise lexicographically first path with a directory component named `torrent` or `torrents`, case-insensitive. This matches whole directory components, not substrings such as `my-torrent-backups`.
5. Process only copies on other disks. Same-disk duplicates and groups without a torrent-priority source are left alone. Other torrent copies on different disks may also be consolidated.

Inventory metadata is held in memory, proportional to file count; file contents are streamed. Hashing and comparisons run sequentially to avoid simultaneous random reads across the array. After initial hashing, each candidate receives one full byte-for-byte comparison before replacement. Once the hardlink is ready, only inode/path identity and file metadata are checked; neither file's contents are read again. This removes the second full comparison from earlier versions while retaining change detection through recorded metadata. All writers and Mover must remain paused.

## Safety and metadata behavior

- Directory descriptors anchor operations to the selected physical disks. Symlinked ancestors, dangling destination symlinks, nested filesystems and destination collisions are rejected.
- A pre-existing destination is accepted only if it is a regular file with the torrent source's device and inode. This can finish an interrupted operation safely after revalidation.
- The script verifies device/inode, size, ownership, mode, modification/change timestamps and link count around reads and before removal. It compares contents byte-for-byte rather than relying on a checksum alone.
- A failed link leaves the original intact. If verification fails before removal, the script attempts to remove only its own newly created link; it retains the original. If original removal has started, the replacement is retained even when interruption, syncing or journaling subsequently fails.
- Both the replacement file and affected directories are synced before the original is removed. The original's parent is synced after removal. Filesystem or hardware failures can still prevent durable writes; inspect the journal after any unsuccessful run.
- Automatic ownership and permission repair is enabled by default. After a byte-for-byte content comparison and destination collision checks, the script resolves the host's `nobody` user and `users` group and repairs participating files to those IDs. On Unraid these are normally UID 99 and GID 100. It resolves account names instead of assuming the IDs.
- Permission repair selects the final mode by filesystem object type. Directories receive `777` directly, preserving traversal throughout repair. Regular files receive `666`, including files that previously had owner-read-only permissions. Special permission bits are cleared. Unsupported file types are rejected. This uses the explicit media-file policy from your directory/file-specific `find` commands.
- Ownership/permission repair is limited to confirmed duplicate participants and their source, original and replacement ancestor directories, down to each top-level share directory. Those directories become `nobody:users` with mode `777`. Disk mount roots are excluded from permission repair. Newly created replacement directories inherit the repaired original directory metadata. Shared ancestor directories may also contain unrelated files, whose own metadata is outside this operation's scope. Existing hardlink aliases share an inode and therefore receive the same file metadata change.
- Directory metadata copying requires directory descriptors for both source and destination. A template mode without any execute bits is rejected before it can be applied to a new directory. With normalization enabled, participating directories are repaired to `777` before copying. With `--no-normalize-metadata`, an unusable template stops processing. New directories start with mode `700`, which already permits owner traversal.
- Unrelated extended attributes are preserved. Access ACL mask changes caused by `chmod` are projected during dry runs and checked again after real repair. Remaining attribute conflicts produce `Skipping extended-attribute mismatch` and are skipped unless `--allow-metadata-change` explicitly permits using the torrent source's attributes. Ownership changes can clear privilege-related attributes according to Linux `chown` behavior.
- Dry runs record and log `WOULD normalize` operations without applying ownership or mode changes. Real runs journal each repair before and after applying it and sync the modified inode. Successfully completed metadata repairs remain in place if a subsequent repair, link or journal operation fails. The original duplicate is retained until a verified replacement exists. Empty directories created by an interrupted run may remain.
- `--no-normalize-metadata` restores strict UID/GID/mode/xattr matching. In that mode, `--allow-metadata-change` permits using the source inode's metadata for a differing duplicate.
- File timestamps become those of the torrent inode. Hardlinks share file data and inode metadata, so modifying one changes every alias.
- The script reads `/boot/config/share.cfg` and `/boot/config/shares/<share>.cfg` to check global/per-share disk inclusion/exclusion and known pool-only settings. Missing global configuration blocks actions. Missing per-share configuration uses Unraid's default share settings. It does not reproduce the full allocation engine or enforce split-level placement; physical creation intentionally chooses the torrent disk.
- The first action failure stops the run with a nonzero status. Earlier completed operations remain completed and are recorded. Metadata skips are recorded without failing the run.

**Concurrency limitation:** validation cannot make a pathname removal atomic with respect to unrelated writers. Pausing all writers and Mover is a requirement. The journal supports inspection and safe revalidation; it is not an automatic rollback tool or a guarantee against abrupt hardware failure.

## Logs, journal and recovery

After scanning, an immediate summary reports the file count. The next message reports candidate paths, unique inodes and the total bytes to hash. All hashing finishes before duplicate actions begin, so large inventories can spend hours in this phase even during a dry run.

At INFO level, a heartbeat runs every 30 seconds by default throughout scanning, grouping, hashing, comparison and replacement. It shows the current phase and path, elapsed time, and (while reading content) bytes processed and file percentage. Hashing also shows completed hashes and completed hash bytes against their totals. The current file's bytes are reported separately from those completed totals. Comparisons show both paths and count matched bytes per file, not the combined physical reads of both files. Hashing completion has its own summary before duplicate processing starts.

The heartbeat uses an independent thread that reads in-memory counters; it does not inspect media or write recovery events. If a read is waiting, messages continue and report how long the byte counter has not advanced. This proves the monitor is responsive, not that storage is making progress. OS scheduling or stalled log output can still delay messages. Paths are escaped as in other log messages. `--progress-interval 10` changes the interval; `--progress-interval 0` disables the heartbeat but retains phase summaries. WARNING/ERROR logging levels suppress these INFO messages.

Action messages explicitly label `original`, `retained_source` and `replacement_hardlink`. The original may be on disk2 while both the retained source and replacement hardlink are on disk1. The replacement preserves the original's relative path on the source disk; it does not create a cross-filesystem hardlink at the original physical path.

Each invocation that passes disk validation creates:

- `deduplicator_<timestamp>_<pid>.log`: readable activity and summary.
- `deduplicator_<timestamp>_<pid>.journal.jsonl`: JSON records with escaped filenames, operation ID, source/original/target paths, SHA-256 and device/inode identities. Real-run events are flushed and synced.

| Event | Meaning |
| --- | --- |
| `WOULD_DEDUPLICATE` | Dry-run candidate. No file was linked or removed. A later preflight error can still reject this candidate. |
| `SKIPPED_METADATA` | Remaining attributes differ, or strict matching is enabled. Original retained. |
| `WOULD_NORMALIZE_METADATA` | Dry-run ownership/mode repair with before/after values. |
| `METADATA_PREPARED` | Ownership/mode repair intent persisted before applying changes. |
| `METADATA_NORMALIZED` | Ownership/mode repair applied, verified and synced. |
| `PREPARED` | Candidate compared; intent persisted before destination creation. |
| `LINK_READY` | Replacement link exists and was synced. |
| `VERIFIED` | File metadata rechecked after linking; `verification="inode-and-metadata"` in v2.3.1. Content comparison already occurred before linking; final path/inode checks still precede removal. Earlier versions also repeated the content comparison here. |
| `ORIGINAL_REMOVED` | Original directory entry removed and its parent synced. |
| `LINK_ROLLED_BACK` | Script removed its newly created link before original removal. |
| `RUN_COMPLETE` | Invocation finished; includes summary counters. |

After interruption or failure, keep writers paused and inspect the latest journal. An operation without `ORIGINAL_REMOVED` can still have completed removal if it stopped between unlinking and recording that event. **Check actual paths and inode identities before taking recovery actions.** Do not blindly replay journal commands or delete either path.

Run a fresh dry-run to re-scan and revalidate the remaining state. A subsequent real run can finish cases where both the verified hardlink and old copy remain. If rollback retained the original and removed the new link, a fresh run can process the original again. Keep journals until the result has been reviewed; remove old journals manually when no longer needed.

The summary reports scanned files, hashed inodes/bytes, duplicate groups, planned or completed operations, metadata repair counts, remaining metadata conflicts and skipped symlinks/nested mounts where applicable. `estimated_reclaimable_bytes` counts allocated blocks only when the removed copy has one remaining link. It is an estimate: reflinks, snapshots, compression, open handles and filesystem accounting can change actual space reclaimed.

## Exit status

| Code | Meaning |
| --- | --- |
| `0` | Successful scan/run, including no duplicates or recorded metadata skips. |
| `1` | Runtime, safety, permission, mount, scan, hash or action failure. Partial completed operations are possible; inspect the journal. |
| `2` | Invalid command-line arguments or unsupported Python version. |
| `130` | Interrupted by SIGINT or SIGTERM; inspect the journal. |

## Operational limits

This version handles **array disks only**. Cache/pool copies are outside the inventory. Existing duplicate logical paths on other unselected disks or pools are not reconciled. Confirm that the logical library path is unambiguous before running against a subset of disks.

Files accessed through physical `/mnt/diskN` paths will move to a different physical path. User-share visibility depends on Unraid share settings. Mover, filesystem conversions and tools that copy/recreate files may change hardlink relationships later; verify them after storage changes.

The script does not configure torrent clients, Radarr, Sonarr, container mappings, or Mover. Run it on the Unraid host with appropriate access. A host command is used here because disk-root and mount validation must see the host's physical filesystems.

## Validation performed

The updated suite ran 82 regression tests, with 81 passing and one live POSIX ACL test skipped because the test filesystem lacks ACL support. Disposable tests used two actual Linux filesystems (`/tmp` and `/dev/shm`) for renamed duplicates, unusual and undecodable filenames, exclusions, NUL input, collisions, symlinks, hardlink identity, metadata handling, multiple aliases, stale/changing files, injected link/unlink/journal failures and interruption windows. The Python entry point was checked with `--help` and `--version`. Added checks cover explicit file/directory mode policy, default repair, dry-run preservation, torrent source priority, directory traversal during interrupted repair, rejection of invalid directory metadata templates, inode snapshot refresh, account resolution, repair failures and ACL preview. Progress checks cover a waiting read, byte counters, escaped paths, configurable intervals, and monitor cleanup. Replacement checks confirm exactly one comparison per duplicate, no content reads after `LINK_READY`, preservation of changed originals, and rejection of a substituted replacement inode. Real ownership changes to UID 99/GID 100 require an Unraid host and were not executed here. Tests inject the test account IDs for filesystem operations and verify the Unraid account lookup separately.

These are Linux filesystem regression tests, not a live Unraid/XFS/Btrfs/ZFS integration test. Review a dry-run on your server before applying changes.
