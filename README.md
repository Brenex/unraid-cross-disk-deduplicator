# Unraid Cross-Disk Deduplicator — v3.0.0

Consolidate identical files from different **physical array disks** onto a retained copy's disk. General mode chooses an existing copy by available disk space; torrent mode retains a file from a master directory. Each replacement preserves the old copy's relative path and uses a same-filesystem hardlink. Default operation is **dry-run**.

The script requires **Python 3.9+** and uses only the standard library.

## General mode: scan a directory across the array

For your books share:

```bash
python3 ./unraid-cross-disk-deduplicator.py --directory /mnt/user/books/ --dry-run
```

The shorter positional form does the same thing:

```bash
python3 ./unraid-cross-disk-deduplicator.py /mnt/user/books/ --dry-run
```

Both recursively scan `/mnt/disk1/books/`, `/mnt/disk2/books/`, and matching paths on the other selected mounted array disks. `/mnt/user` is used to describe the scope, not to read, write or hardlink files through the merged filesystem. Directories absent on some disks are normal; a requested directory absent on every selected array disk is an error. Cache/pool files are outside this tool's scope.

At the start of each run, all selected array disks are polled once using their physical filesystem descriptors. Logs show total, used and available bytes. For every group of content-identical files, general mode retains an existing copy on the disk with the most **available bytes**, regardless of free percentage. It processes copies on other disks from least to most available space. The ranking is fixed for that run. Ties use disk number, then bytewise path order.

For example, if disk3 is roomier and these files have identical content:

```text
Before: /mnt/disk1/books/Author/book.pdf
        /mnt/disk3/books/Other/renamed.pdf
After:  /mnt/disk3/books/Author/book.pdf
        /mnt/disk3/books/Other/renamed.pdf
```

The two remaining paths share the retained inode. Both logical `/mnt/user/books/...` paths are preserved. Files do not need matching names or relative paths. SHA-256 and a single byte-for-byte comparison establish equality. No second full content comparison is performed after linking.

This is cross-disk consolidation. Independent duplicates already on the retained disk are left alone. The script chooses among disks that already contain a matching copy; it does not copy data to an unrelated empty disk or move unique files for balancing. A destination already occupied by a different inode stops that action, even if its contents match; same-disk repair is outside this mode.

After reviewing the dry-run, use the same arguments with `--real-run` while writers and Mover remain paused.

## Torrent mode: retain a master copy

Supply the directory to deduplicate and a configurable torrent master directory:

```bash
python3 ./unraid-cross-disk-deduplicator.py --mode torrent \
  --directory /mnt/user/books \
  --master-dir /mnt/user/data/torrents --dry-run
```

The master tree is scanned in addition to the input tree, even when they do not overlap. Only groups with matching master content are eligible. A master copy wins even if its disk is fuller than the other disks. If multiple masters match, their disks are ranked by initial available bytes, then disk number and path. Configured master directory names can be anything; they do not have to be named `torrent` or `torrents`.

Files outside the input directories are reference candidates and are never replacement/removal targets. The retained source and its parent directories can still receive the existing automatic permission repair. If an input directory itself includes a master directory, other master copies within that input scope may be consolidated while preserving their logical paths. General duplicates without a matching master are skipped in torrent mode. The script does not query a torrent client or verify tracker membership; configure the master tree to contain the files you can recheck in your client.

`--directory` and `--master-dir` are repeatable. Logical paths expand across selected array disks. A physical path passed with either flag, such as `/mnt/disk2/books`, is limited to that particular selected disk. Symlinked or nested-filesystem paths are rejected. Overlapping scopes are walked once.

| Invocation | Mode and retained copy |
| --- | --- |
| `--directory /mnt/user/books` or positional `/mnt/user/books` | General mode; existing copy on the disk with the most available bytes. |
| `--mode torrent --directory PATH --master-dir MASTER` | Torrent mode; copy under a configured master, then most available bytes. `--master-dir` alone also implies torrent mode. |
| `--mode torrent --directory PATH` | Torrent mode; requires a whole directory component named `torrent` or `torrents` within the scan. |
| Old commands without directory/master arguments | Backward-compatible torrent mode; lexicographically first torrent-priority path. |
| `--mode general` without directory arguments | General mode over all selected array disks and shares. |

Physical mount roots remain positional arguments to limit which disks participate. For example, append `/mnt/disk1 /mnt/disk3` to either mode. Explicit masters without input directories scan all selected array disks and shares. File-list mode defines the exact inventory and cannot be combined with directory/master scopes.

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
4. Use `/mnt/user/<share>/...` with `--directory` or as a positional input directory. Physical `/mnt/diskN` mount roots optionally limit participating disks. `/mnt/user0`, cache/pool mounts, Unassigned Devices and symlinked roots are unsupported. Physical subdirectories are accepted through directory/master flags, not as disk-root arguments.
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
| `--mode general\|torrent` | General: any duplicate content, retaining the roomiest existing copy. Torrent: require a master copy. A directory defaults to general; master arguments imply torrent; old commands remain torrent mode. |
| `--directory PATH`, `--source-dir PATH` | Recursive input directory; repeatable. `/mnt/user/<share>/...` maps across disks; a selected physical disk subdirectory is pinned to that disk. Positional `/mnt/user/...` directories are also accepted. |
| `--master-dir PATH` | Configurable master directory for torrent mode; repeatable and scanned in addition to inputs. Master matches take priority over general free-space ranking. |
| `-p FILE`, `--process-file FILE` | Read absolute physical disk paths from an LF-delimited file. A final LF is optional. Cannot be combined with directory/master scopes. |
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
2. Group files by size. Hash only size groups spanning disks and containing an input candidate. Torrent mode additionally requires a master candidate; general mode has no torrent-path requirement. Matching basenames are **not** required.
3. Calculate SHA-256 using streamed reads. Existing hardlink aliases share a checksum calculation when their inode and recorded metadata match. A hashing error aborts the whole plan before mutations.
4. Group matching content and choose the retained source according to the selected mode and startup space snapshot. Legacy torrent detection matches whole `torrent`/`torrents` directory components, case-insensitively, not substrings such as `my-torrent-backups`.
5. Process input copies on other disks, fullest first within each group. Same-disk duplicates are left alone. In torrent mode, groups without a master are left alone and reference copies outside input scopes are retained.

Inventory metadata is held in memory, proportional to file count; file contents are streamed. Hashing and comparisons run sequentially to avoid simultaneous random reads across the array. After initial hashing, each candidate receives one full byte-for-byte comparison before replacement. Once the hardlink is ready, only inode/path identity and file metadata are checked; neither file's contents are read again. This removes the second full comparison from earlier versions while retaining change detection through recorded metadata. All writers and Mover must remain paused.

## Safety and metadata behavior

- Directory descriptors anchor operations to the selected physical disks. Symlinked ancestors, dangling destination symlinks, nested filesystems and destination collisions are rejected.
- A pre-existing destination is accepted only if it is a regular file with the retained source's device and inode. This can finish an interrupted operation safely after revalidation. An explicitly excluded replacement path is rejected before metadata changes or linking.
- The script verifies device/inode, size, ownership, mode, modification/change timestamps and link count around reads and before removal. It compares contents byte-for-byte rather than relying on a checksum alone.
- A failed link leaves the original intact. If verification fails before removal, the script attempts to remove only its own newly created link; it retains the original. If original removal has started, the replacement is retained even when interruption, syncing or journaling subsequently fails.
- Both the replacement file and affected directories are synced before the original is removed. The original's parent is synced after removal. Filesystem or hardware failures can still prevent durable writes; inspect the journal after any unsuccessful run.
- Automatic ownership and permission repair is enabled by default. After a byte-for-byte content comparison and destination collision checks, the script resolves the host's `nobody` user and `users` group and repairs participating files to those IDs. On Unraid these are normally UID 99 and GID 100. It resolves account names instead of assuming the IDs.
- Permission repair selects the final mode by filesystem object type. Directories receive `777` directly, preserving traversal throughout repair. Regular files receive `666`, including files that previously had owner-read-only permissions. Special permission bits are cleared. Unsupported file types are rejected. This uses the explicit media-file policy from your directory/file-specific `find` commands.
- Ownership/permission repair is limited to confirmed duplicate participants and their source, original and replacement ancestor directories, down to each top-level share directory. Those directories become `nobody:users` with mode `777`. Disk mount roots are excluded from permission repair. Newly created replacement directories inherit the repaired original directory metadata. Shared ancestor directories may also contain unrelated files, whose own metadata is outside this operation's scope. Existing hardlink aliases share an inode and therefore receive the same file metadata change.
- Directory metadata copying requires directory descriptors for both source and destination. A template mode without any execute bits is rejected before it can be applied to a new directory. With normalization enabled, participating directories are repaired to `777` before copying. With `--no-normalize-metadata`, an unusable template stops processing. New directories start with mode `700`, which already permits owner traversal.
- Unrelated extended attributes are preserved. Access ACL mask changes caused by `chmod` are projected during dry runs and checked again after real repair. Remaining attribute conflicts produce `Skipping extended-attribute mismatch` and are skipped unless `--allow-metadata-change` explicitly permits using the retained source's attributes. Ownership changes can clear privilege-related attributes according to Linux `chown` behavior.
- Dry runs record and log `WOULD normalize` operations without applying ownership or mode changes. Real runs journal each repair before and after applying it and sync the modified inode. Successfully completed metadata repairs remain in place if a subsequent repair, link or journal operation fails. The original duplicate is retained until a verified replacement exists. Empty directories created by an interrupted run may remain.
- `--no-normalize-metadata` restores strict UID/GID/mode/xattr matching. In that mode, `--allow-metadata-change` permits using the source inode's metadata for a differing duplicate.
- File timestamps become those of the retained inode. Hardlinks share file data and inode metadata, so modifying one changes every alias.
- The script reads `/boot/config/share.cfg` and `/boot/config/shares/<share>.cfg` to check global/per-share disk inclusion/exclusion and known pool-only settings. Missing global configuration blocks actions. Missing per-share configuration uses Unraid's default share settings. It does not reproduce the full allocation engine or enforce split-level placement; physical creation intentionally chooses the retained copy's disk. A share-policy conflict stops the operation rather than silently selecting an otherwise disallowed destination.
- The first action failure stops the run with a nonzero status. Earlier completed operations remain completed and are recorded. Metadata skips are recorded without failing the run.

**Concurrency limitation:** validation cannot make a pathname removal atomic with respect to unrelated writers. Pausing all writers and Mover is a requirement. The journal supports inspection and safe revalidation; it is not an automatic rollback tool or a guarantee against abrupt hardware failure.

## Logs, journal and recovery

Before scanning, disk-usage messages report the initial available bytes used for source selection. After scanning, an immediate summary reports the file count. The next message reports candidate paths, unique inodes and the total bytes to hash. All hashing finishes before duplicate actions begin, so large inventories can spend hours in this phase even during a dry run. Each duplicate group logs its retained source, selection policy and initial available space.

At INFO level, a heartbeat runs every 30 seconds by default throughout scanning, grouping, hashing, comparison and replacement. It shows the current phase and path, elapsed time, and (while reading content) bytes processed and file percentage. Hashing also shows completed hashes and completed hash bytes against their totals. The current file's bytes are reported separately from those completed totals. Comparisons show both paths and count matched bytes per file, not the combined physical reads of both files. Hashing completion has its own summary before duplicate processing starts.

The heartbeat uses an independent thread that reads in-memory counters; it does not inspect media or write recovery events. If a read is waiting, messages continue and report how long the byte counter has not advanced. This proves the monitor is responsive, not that storage is making progress. OS scheduling or stalled log output can still delay messages. Paths are escaped as in other log messages. `--progress-interval 10` changes the interval; `--progress-interval 0` disables the heartbeat but retains phase summaries. WARNING/ERROR logging levels suppress these INFO messages.

Action messages explicitly label `original`, `retained_source` and `replacement_hardlink`. The original may be on disk2 while both the retained source and replacement hardlink are on disk1. The replacement preserves the original's relative path on the source disk; it does not create a cross-filesystem hardlink at the original physical path.

Each invocation that passes disk validation creates:

- `deduplicator_<timestamp>_<pid>.log`: readable activity and summary.
- `deduplicator_<timestamp>_<pid>.journal.jsonl`: JSON records with escaped filenames, operation ID, source/original/target paths, SHA-256 and device/inode identities. Real-run events are flushed and synced.

| Event | Meaning |
| --- | --- |
| `DISK_USAGE_SNAPSHOT` | Initial total/used/available bytes for selected disks and the selection metric. Action records also contain the mode and source/original initial available bytes. |
| `WOULD_DEDUPLICATE` | Dry-run candidate. No file was linked or removed. A later preflight error can still reject this candidate. |
| `SKIPPED_METADATA` | Remaining attributes differ, or strict matching is enabled. Original retained. |
| `WOULD_NORMALIZE_METADATA` | Dry-run ownership/mode repair with before/after values. |
| `METADATA_PREPARED` | Ownership/mode repair intent persisted before applying changes. |
| `METADATA_NORMALIZED` | Ownership/mode repair applied, verified and synced. |
| `PREPARED` | Candidate compared; intent persisted before destination creation. |
| `LINK_READY` | Replacement link exists and was synced. |
| `VERIFIED` | File metadata rechecked after linking; `verification="inode-and-metadata"` in v2.3.1 and later. Content comparison already occurred before linking; final path/inode checks still precede removal. Older versions also repeated the content comparison here. |
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

Run the regression suite from the repository root:

```bash
python3 -m unittest discover -s tests -v
```

The updated suite ran 112 regression tests, with 111 passing and one live POSIX ACL test skipped because the test filesystem lacks ACL support. Disposable tests used two actual Linux filesystems (`/tmp` and `/dev/shm`) for renamed duplicates, unusual and undecodable filenames, exclusions, NUL input, collisions, symlinks, hardlink identity, metadata handling, multiple aliases, stale/changing files, injected link/unlink/journal failures and interruption windows. The Python entry point was checked with `--help` and `--version`. Added checks cover explicit file/directory mode policy, default repair, dry-run preservation, torrent source priority, directory traversal during interrupted repair, rejection of invalid directory metadata templates, inode snapshot refresh, account resolution, repair failures and ACL preview. Progress checks cover a waiting read, byte counters, escaped paths, configurable intervals, and monitor cleanup. Replacement checks confirm exactly one comparison per duplicate, no content reads after `LINK_READY`, preservation of changed originals, and rejection of a substituted replacement inode. Directory and selection checks cover logical-to-physical expansion, recursive scope boundaries, overlapping scopes, exclusions, missing and symlinked scopes, physical-directory inputs, general mode without torrent names, same-path duplicates, free bytes versus percentages, fixed startup snapshots, deterministic ties, fullest-first processing, custom masters outside the input tree, unmatched torrent groups, and preserved legacy commands. Real ownership changes to UID 99/GID 100 require an Unraid host and were not executed here. Tests inject the test account IDs for filesystem operations and verify the Unraid account lookup separately.

These are Linux filesystem regression tests, not a live Unraid/XFS/Btrfs/ZFS integration test. Review a dry-run on your server before applying changes.
