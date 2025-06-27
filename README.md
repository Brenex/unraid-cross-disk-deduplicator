# Unraid Cross-Disk Deduplicator

This Bash script is designed to identify and manage duplicate files across multiple specified disk mounts, particularly useful for Unraid users. It prioritizes files located in paths containing "torrent" or "torrents" and replaces other identical files on different disks with hardlinks to the prioritized version. This strategy helps in deduplicating data and saving disk space, especially beneficial for managing large media collections often associated with torrent clients.

The script offers robust logging and includes a crucial dry-run mode for testing before any modifications are made to your file system.

## Features

- **Cross-Disk Deduplication:** Identifies and processes duplicate files that span across different disk mounts.
- **Torrent Priority:** Prioritizes versions of duplicate files found within "torrent" or "torrents" paths (case-insensitive).
- **Hardlinking:** Replaces non-priority duplicates with hardlinks to the chosen priority file, preserving data integrity while freeing up space.
- **Dry Run Mode:** Default operation mode that simulates all actions without making any changes, providing detailed logs of what *would* happen.
- **Configurable Disk Mounts:** Easily define the disk paths to scan.
- **Flexible Input:** Can scan configured disk mounts or process a list of file paths from an input file.
- **Comprehensive Logging:** Outputs messages to both console and timestamped log files with configurable verbosity.
- **Automated Log Cleanup:** Manages old log files to prevent excessive disk usage.
- **Root Privilege Enforcement:** Ensures the script is run with necessary permissions for file operations.

## Prerequisites

- **Operating System:** Designed for Linux-based systems, specifically tested for environments like Unraid.
- **Permissions:** Must be run with `root` privileges (`sudo`) to perform file deletions and create hardlinks across different mount points.
- **Core Utilities:** The following commands must be available in your system's PATH:
	- `find`
	- `xargs`
	- `awk`
	- `mkdir`
	- `rm`
	- `ln`
	- `mktemp`
	- `dirname`
	- `readlink`
	- `date`
	- `ls`
	- `tail`
	- `tee`

## Installation

1. **Download:** Clone this repository or download the `unraid-cross-disk-deduplicator.sh` script to your desired location on your Unraid server or other Linux system.
	```
	git clone https://github.com/your-repo/unraid-cross-disk-deduplicator.git
	cd unraid-cross-disk-deduplicator
	```
	(Or simply copy the script file.)
2. **Make Executable:** Grant execute permissions to the script.
	```
	chmod +x unraid-cross-disk-deduplicator.sh
	```

## Configuration

Open the `unraid-cross-disk-deduplicator.sh` script in a text editor to adjust the following variables:

```
# --- Configuration ---
declare -a DISK_MOUNTS=("/mnt/disk3" "/mnt/disk4" "/mnt/disk5" "/mnt/disk6" "/mnt/disk7")
# Define the disk paths your script should scan for files.
# For Unraid, these typically follow the /mnt/diskX pattern.

# Logging directory (relative to the script's location)
SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
LOGS_BASE_DIR="$SCRIPT_DIR/logs"
# The directory where log files will be stored. Defaults to a 'logs' subdirectory
# relative to where the script is executed.

# Number of log files to retain (e.g., last 5 logs)
NUM_LOGS_TO_RETAIN=5
# Configure how many historical log files to keep. Older logs will be automatically cleaned up.

# --- Log Level Configuration ---
# Define log level severity: DEBUG < INFO < WARNING < ERROR < CRITICAL < NONE
# Messages with a level higher than or equal to the threshold will be shown.
declare -A LOG_LEVELS=( ["DEBUG"]=10 ["INFO"]=20 ["WARNING"]=30 ["ERROR"]=40 ["CRITICAL"]=50 ["NONE"]=99 )

# Default log level for console output (e.g., INFO, WARNING, ERROR)
CONSOLE_LOG_LEVEL_THRESHOLD="INFO"
# Messages at or above this level will be printed to your terminal.

# Default log level for file output (e.g., DEBUG, INFO)
FILE_LOG_LEVEL_THRESHOLD="INFO"
# Messages at or above this level will be written to the log file.
# For detailed debugging, set this to "DEBUG".
```

## Usage

It is **highly recommended** to run the script in `dry-run` mode first to understand its actions before enabling `real-run` mode.

```
sudo ./unraid-cross-disk-deduplicator.sh [OPTIONS]
```

### Options

- `-r`, `--real-run`
	- **Description:** Enables real run mode. Files will be **deleted** and hardlinks created.
	- **Caution:** USE WITH EXTREME CAUTION. Backups are highly recommended before using this mode.
- `-d`, `--dry-run`
	- **Description:** Enables dry run mode (this is the default behavior if no mode is specified). No files will be deleted or hardlinks created. The script will only log what it *would* do.
- `-p <input_file>`, `--process-file <input_file>`
	- **Description:** Processes file paths from the specified input file instead of scanning the configured `DISK_MOUNTS`. The input file should contain one absolute file path per line. This is useful for processing a pre-generated list of files or for re-running on a specific subset.
- `-v <LOG_LEVEL>`, `--verbose <LOG_LEVEL>`
	- **Description:** Set the minimum log level for both console and file output.
	- **Valid levels:** `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`.
	- **Default for console:** `INFO`
	- **Default for file:** `INFO`
	- **Example:** `-v DEBUG` will show all debug messages.

### Examples

**1\. Dry Run (Default - Recommended First Step):** Scan configured disks, identify duplicates, and log potential actions without making any changes.

```
sudo ./unraid-cross-disk-deduplicator.sh
```

**2\. Real Run (With Caution):** Scan configured disks and perform actual deletions and hardlink creations. **Ensure you have backups.**

```
sudo ./unraid-cross-disk-deduplicator.sh --real-run
```

**3\. Process Files from an Input List (Dry Run):** Read file paths from `/tmp/my_files_to_check.txt` and log potential actions.

```
sudo ./unraid-cross-disk-deduplicator.sh -p /tmp/my_files_to_check.txt
```

**4\. Real Run with Debug Logging:** Execute the script, performing actual changes, and output highly verbose debug messages to both console and log file.

```
sudo ./unraid-cross-disk-deduplicator.sh -r -v DEBUG
```

## How It Works

1. **Argument Parsing & Pre-checks:** The script parses command-line arguments to determine the run mode (dry vs. real), log level, and whether to scan disks or use an input file. It also performs a root privilege check.
2. **File Collection:**
	- If no input file is specified, it scans the `DISK_MOUNTS` array using `find` to collect all file paths, writing them to an ephemeral temporary file in `/tmp`. This approach is robust against filenames with spaces or special characters.
	- If an input file is provided, it uses that file as the source of file paths.
3. **Duplicate Identification & Prioritization (AWK):**
	- The collected file paths are piped into an `awk` script.
	- `awk` groups files by filename.
	- It then filters for groups where duplicates exist across **different disk mounts**.
	- Within each cross-disk duplicate group, it identifies a "priority file" by searching for paths containing `/torrent/` or `/torrents/` (case-insensitive).
	- `awk` outputs formatted lines indicating the priority file and other duplicate files that are candidates for deletion and hardlinking (only if they are on a *different* disk mount than the priority file). It also identifies groups where no torrent priority file was found.
4. **Action Execution (Bash Loop):**
	- The output from `awk` is read line by line by a `while read` loop in Bash.
	- For each `DELETE_AND_HARDLINK` instruction:
		- It performs essential pre-checks:
			- Extracts the mount points of both the priority file and the file to be deleted.
			- Calculates the target directory path for the new hardlink on the *priority file's disk*.
			- Creates the necessary target directories on the priority disk if they don't exist.
			- Checks for existing files at the hardlink target path to prevent conflicts.
		- If all pre-checks pass, it proceeds:
			- **Deletes** the duplicate file.
			- **Creates a hardlink** from the original priority file to the location where the duplicate was, effectively linking the old path to the priority file's inode.
		- All actions are logged according to the configured log level and run mode.
5. **Cleanup (Trap):** An `EXIT` trap ensures that the temporary file created during the disk scan is always deleted upon script exit, regardless of success or failure. It also triggers the `cleanup_old_logs` function.
6. **Log Retention:** The `cleanup_old_logs` function runs on exit, removing older log files and retaining only the most recent `NUM_LOGS_TO_RETAIN` logs.

## Important Notes & Warnings

- **BACKUP YOUR DATA:** Before running this script in `real-run` mode, ensure you have a complete and verified backup of your data. While hardlinks are generally safe, incorrect usage or unforeseen circumstances could lead to data loss.
- **Hardlinks Explained:** Hardlinks are direct pointers to the same data on a file system. If you modify one hardlink, all other hardlinks pointing to the same data will reflect those changes. Deleting a hardlink only removes that specific directory entry; the data itself is only removed from the disk when the last hardlink pointing to it is deleted.
- **Cross-Filesystem Limitations:** Hardlinks can only exist within the same file system (i.e., on the same disk mount). This script specifically targets duplicates that span *different* disk mounts by deleting the duplicate and creating a new hardlink on the *priority file's disk* that points to the priority file's data.
- **Space Savings:** This script saves space by replacing full copies of files with tiny hardlink entries. The actual space is freed up once the original duplicate file (not the hardlink) is deleted.
- **Torrent Client Compatibility:** This script is particularly useful for torrent environments because torrent clients typically verify files based on content hashes. By replacing duplicates with hardlinks to a "master" torrent file, you ensure that your torrent client continues to seed and verify the file correctly, even though the physical storage location has been deduplicated on other disks.
- **Error Handling:** The script includes robust error handling and logging. Monitor the console output and the generated log files in the `logs` directory for any warnings or errors.
- **Idempotency:** The script is largely idempotent, meaning running it multiple times on the same dataset should yield the same result without unintended side effects, as it will only process new duplicates or correct previously failed operations.

## Exit Codes

- `0`: Script completed successfully.
- `1`: Script encountered an error or was run without root privileges.

## Development & Contribution

Feel free to open issues or submit pull requests if you have suggestions for improvements, bug fixes, or new features.
