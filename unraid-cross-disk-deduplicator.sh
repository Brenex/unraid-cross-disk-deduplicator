#!/bin/bash
set -euo pipefail
# bash_duplicate_cleanup.sh
#
# This script identifies and manages duplicate files across specified disk mounts.
# It prioritizes files located in paths containing "torrent" or "torrents" and
# replaces other identical files on different disks with hardlinks to the
# prioritized version. This helps in deduplicating data and saving disk space,
# particularly useful for media collections managed by torrent clients.
#
# Usage:
#   sudo ./cross_disk_deduplicator.sh [-r|--real-run] [-d|--dry-run] [-p <input_file>] [-v <LOG_LEVEL>]
#
# Options:
#   -r, --real-run       : Enable real run mode. Files will be deleted and hardlinks created.
#                          USE WITH CAUTION. Backups are highly recommended.
#   -d, --dry-run        : Enable dry run mode (default). No files will be deleted or
#                          hardlinks created. The script will only log what it *would* do.
#   -p <input_file>      : Process file paths from the specified input file instead
#                          of scanning the configured disk mounts. The input file
#                          should contain one file path per line.
#   -v <LOG_LEVEL>       : Set the minimum log level for console and file output.
#                          Valid levels: DEBUG, INFO, WARNING, ERROR, CRITICAL.
#                          Default for console: INFO, Default for file: INFO.
#
# Prerequisites:
#   - Must be run with root privileges (sudo) to perform file deletions and
#     create hardlinks across different mount points.
#   - `find`, `xargs`, `awk`, `mkdir`, `rm`, `ln` commands must be available.
#
# Configuration:
#   - DISK_MOUNTS: Array of disk paths to scan for files.
#   - NUM_LOGS_TO_RETAIN: Number of old log files to keep.
#
# Output:
#   - Logs messages to the console and to a timestamped log file in the 'logs' directory.
#
# Exit Codes:
#   - 0: Script completed successfully.
#   - 1: Script encountered an error or was run without root privileges.
#

# --- Configuration ---
declare -a DISK_MOUNTS=("/mnt/disk3" "/mnt/disk4" "/mnt/disk5" "/mnt/disk6" "/mnt/disk7")

# Logging directory (relative to the script's location)
SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
LOGS_BASE_DIR="$SCRIPT_DIR/logs"

# Number of log files to retain (last 5 logs)
NUM_LOGS_TO_RETAIN=5

# --- Log Level Configuration ---
# Define log level severity: DEBUG < INFO < WARNING < ERROR < CRITICAL < NONE
# Messages with a level higher than or equal to the threshold will be shown.
declare -A LOG_LEVELS=( ["DEBUG"]=10 ["INFO"]=20 ["WARNING"]=30 ["ERROR"]=40 ["CRITICAL"]=50 ["NONE"]=99 )

# Default log level for console output (e.g., INFO, WARNING, ERROR)
CONSOLE_LOG_LEVEL_THRESHOLD="INFO"

# Default log level for file output (e.g., DEBUG, INFO)
FILE_LOG_LEVEL_THRESHOLD="INFO"

# Global variable for the current run's log file path
CURRENT_RUN_LOG_FILE=""

# --- Function to get the path for the current run's log file ---
get_new_log_file_path() {
    local timestamp=$(date +'%Y-%m-%d_%H-%M-%S')
    echo "$LOGS_BASE_DIR/duplicate_cleanup_${timestamp}.log"
}

# Global variable for the current run's log file path (INITIALIZED EARLY)
CURRENT_RUN_LOG_FILE=$(get_new_log_file_path)

# --- Logging Function ---
# Usage: log_message <LEVEL> <MESSAGE>
# Example: log_message INFO "Script started."
# Example: log_message ERROR "File not found."
log_message() {
    local level_str="$1"
    shift # Remove the first argument (level string)
    local message="$@" # Remaining arguments form the message

    local current_level_num=${LOG_LEVELS[$level_str]:-0} # Get numeric value, default to 0 if not found
    local console_threshold_num=${LOG_LEVELS[$CONSOLE_LOG_LEVEL_THRESHOLD]:-0}
    local file_threshold_num=${LOG_LEVELS[$FILE_LOG_LEVEL_THRESHOLD]:-0}

    # Ensure the log directory exists. Runs with sudo to handle permissions if needed.
    sudo mkdir -p "$LOGS_BASE_DIR"

    local timestamp_prefix="$(date +'%Y-%m-%d %H:%M:%S')"
    local formatted_message="$timestamp_prefix - $level_str - $message"

    # Write to the current run's specific log file if level is high enough
    if [[ $current_level_num -ge $file_threshold_num ]]; then
        # Use tee to append and create file if it doesn't exist, suppressing tee's stdout
        echo "$formatted_message" | sudo tee -a "$CURRENT_RUN_LOG_FILE" >/dev/null
    fi

    # Also echo to console for immediate feedback if level is high enough
    if [[ $current_level_num -ge $console_threshold_num ]]; then
        echo "$formatted_message"
    fi
}

# --- Dry Run Mode Initialization (Default: true) ---
DRY_RUN=true

# --- Function to clean up old logs based on count ---
cleanup_old_logs() {
    log_message INFO "Cleaning up old logs in '$LOGS_BASE_DIR' (retaining last $NUM_LOGS_TO_RETAIN logs)."
    
    # List all log files, sort by modification time (newest first), and keep only the oldest ones beyond retention limit
    local files_to_delete=$(ls -t "$LOGS_BASE_DIR"/duplicate_cleanup_*.log 2>/dev/null | tail -n +$((NUM_LOGS_TO_RETAIN + 1)))

    if [[ -n "$files_to_delete" ]]; then
        log_message INFO "Deleting old log files:"
        echo "$files_to_delete" | while IFS= read -r file; do
            log_message INFO "  - $file"
            if [[ "$DRY_RUN" == "true" ]]; then
                log_message DEBUG "  DRY RUN: Would delete log: rm -f \"$file\""
            else
                rm -f "$file"
            fi
        done
    else
        log_message INFO "No old log files to delete."
    fi
    log_message INFO "Old log cleanup complete."
}

# --- Input File Processing Flag ---
PROCESS_INPUT_FILE="" # Stores the path to the input file if provided

# --- Global variable for ephemeral temporary file ---
# This ephemeral temporary file in /tmp stores file paths when scanning disks.
TEMP_FILE=""

# --- Argument Parsing ---
# Using getopts for more robust argument parsing
while (( "$#" )); do
    case "$1" in
        -r|--real-run)
            DRY_RUN=false
            log_message INFO "Real run mode enabled. Files will be deleted and hardlinks created."
            shift
            ;;
        -d|--dry-run)
            DRY_RUN=true
            log_message INFO "Dry run mode explicitly enabled."
            shift
            ;;
        -p|--process-file) # Standardizing on `--process-file`
            if [[ -n "$2" && "$2" != -* ]]; then
                PROCESS_INPUT_FILE=$2
                log_message INFO "Processing from input file: '$PROCESS_INPUT_FILE'. Skipping disk scan."
                shift 2
            else
                log_message ERROR "Option -$1 requires an argument."
                echo "Usage: sudo $0 [-r|--real-run] [-d|--dry-run] [-p <input_file>|--process-file <input_file>] [-v <LOG_LEVEL>]" >&2
                exit 1
            fi
            ;;
        -v|--verbose) # Added verbose option for log level control
            if [[ -n "$2" && "$2" != -* ]]; then
                case "$2" in
                    "DEBUG"|"INFO"|"WARNING"|"ERROR"|"CRITICAL")
                        CONSOLE_LOG_LEVEL_THRESHOLD="$2"
                        FILE_LOG_LEVEL_THRESHOLD="$2" # For simplicity, set file log level to match console, or always DEBUG for file if preferred.
                        log_message INFO "Log level set to: $2"
                        ;;
                    *)
                        log_message ERROR "Invalid log level: $2. Must be one of DEBUG, INFO, WARNING, ERROR, CRITICAL."
                        echo "Usage: sudo $0 [-r|--real-run] [-d|--dry-run] [-p <input_file>|--process-file <input_file>] [-v <LOG_LEVEL>]" >&2
                        exit 1
                        ;;
                esac
                shift 2
            else
                log_message ERROR "Option -$1 requires an argument."
                echo "Usage: sudo $0 [-r|--real-run] [-d|--dry-run] [-p <input_file>|--process-file <input_file>] [-v <LOG_LEVEL>]" >&2
                exit 1
            fi
            ;;
        *) # Presume a non-option argument
            log_message ERROR "Invalid argument: $1"
            echo "Usage: sudo $0 [-r|--real-run] [-d|--dry-run] [-p <input_file>|--process-file <input_file>] [-v <LOG_LEVEL>]" >&2
            exit 1
            ;;
    esac
done
shift $((OPTIND -1)) # Shift positional parameters so $1 refers to the first non-option argument

# --- Pre-checks ---
if [[ $(id -u) -ne 0 ]]; then
    echo "This script needs to be run with root privileges to delete files and create hardlinks across mount points."
    echo "To run in dry-run mode (default): 'sudo bash $0'"
    echo "To perform actual changes: 'sudo bash $0 --real-run'"
    echo "To process from a file: 'sudo bash $0 --process-file /path/to/your/input.tmp'"
    echo "To set log level: 'sudo bash $0 -v DEBUG'"
    exit 1
fi

# Log initial status, including run mode
log_message INFO "--- Starting Duplicate File Cleanup Script ---"
if [[ "$DRY_RUN" == "true" ]]; then
    log_message INFO "Dry run mode enabled. No files will be deleted or hardlinks created."
else
    log_message WARNING "Real run mode enabled. Files WILL BE DELETED and hardlinks created."
    log_message WARNING "Please ensure you have backups and understand the script's actions."
fi


# --- Main Script Logic ---

# Conditional execution: Scan disks or process provided input file
if [[ -z "$PROCESS_INPUT_FILE" ]]; then
    # Perform disk scan and generate a NEW temporary file in /tmp
    TEMP_FILE=$(mktemp) # Create a new temporary file in /tmp
    if [[ ! -f "$TEMP_FILE" ]]; then
        log_message ERROR "Failed to create temporary file in /tmp. Exiting."
        exit 1
    fi
    log_message INFO "Collecting all file paths from specified disk mounts (output to ephemeral temp file: '$TEMP_FILE')..."
    
    # Clear previous content of the ephemeral temporary file (it should be empty, but for safety)
    # No DRY_RUN check here, as mktemp creates a fresh file each time.
    > "$TEMP_FILE" 
    
    for disk in "${DISK_MOUNTS[@]}"; do
        if [[ -d "$disk" ]]; then
            log_message INFO "Searching in '$disk'..."
            # Use find with -print0 and xargs -0 for robust handling of filenames with spaces or special characters
            # Append output to the temporary file, redirecting stderr to a log file to capture find/xargs errors
            if ! find "$disk" -type f -print0 | xargs -0 -I {} bash -c 'printf "%s\n" "$@"' _ {} >> "$TEMP_FILE" 2>> "$CURRENT_RUN_LOG_FILE"; then
                log_message WARNING "Errors encountered during scan of '$disk'. Check log file for details."
            fi
        else
            log_message WARNING "Disk mount '$disk' does not exist or is not a directory. Skipping."
        fi
    done
    
    log_message INFO "Disk scan complete. Processing data from '$TEMP_FILE'."
    INPUT_SOURCE="$TEMP_FILE"

else
    # Use provided input file
    if [[ ! -f "$PROCESS_INPUT_FILE" ]]; then
        log_message ERROR "Input file '$PROCESS_INPUT_FILE' not found. Exiting."
        exit 1
    fi
    log_message INFO "Processing data from provided input file: '$PROCESS_INPUT_FILE'."
    INPUT_SOURCE="$PROCESS_INPUT_FILE"
    # Ensure TEMP_FILE is not set, so the trap doesn't try to delete a non-existent file
    TEMP_FILE="" 
fi

# --- Trap for cleanup on exit ---
# Ensures that the temporary file (if created by the script) is deleted
# and old log files are cleaned up upon script exit, regardless of success or failure.
trap "
    log_message INFO 'Script exiting.';
    if [[ -n \"$TEMP_FILE\" && -f \"$TEMP_FILE\" ]]; then # Check if TEMP_FILE was set and exists
        if [[ \"$DRY_RUN\" == \"true\" ]]; then
            log_message DEBUG \"DRY RUN: Would delete ephemeral temporary file: rm -f '$TEMP_FILE'\";
        fi;
        rm -f '$TEMP_FILE'; # ALWAYS delete the ephemeral temp file
    fi;
    cleanup_old_logs;
" EXIT


# AWK processing is now performed on the chosen INPUT_SOURCE
awk -F'/' '
    BEGIN {
        FS="/";
        OFS="/";
    }
    {
        # Trim leading/trailing whitespace to ensure clean paths
        sub(/^[[:space:]]+/, "", $0);
        gsub(/[[:space:]]+$/, "", $0);

        filename = $NF; # Extract filename (last field after /)
        paths[filename][++count[filename]] = $0; # Store full path, indexed by filename and a counter
    }
    END {
        for (filename in paths) {
            if (count[filename] > 1) { # Check if this filename has duplicates
                # --- Determine if duplicates span multiple disk mounts ---
                num_unique_mounts = 0;
                delete unique_mounts_in_group; # Reset for each filename group
                for (i = 1; i <= count[filename]; i++) {
                    current_file = paths[filename][i];
                    # Extract the /mnt/diskX part
                    match(current_file, /^\/mnt\/disk[0-9]+/, current_mount_arr);
                    current_mount = current_mount_arr[0];
                    if (current_mount != "") {
                        if (!(current_mount in unique_mounts_in_group)) {
                            unique_mounts_in_group[current_mount] = 1;
                            num_unique_mounts++;
                        }
                    }
                }

                if (num_unique_mounts <= 1) {
                    # All duplicates for this filename are on the same disk, skip.
                    # We only care about duplicates that span across different mounts.
                    continue;
                }

                # --- Duplicates found spanning multiple disks, now prioritize ---
                torrent_file = "";

                # First pass: Find the "torrent" priority file (case-insensitive)
                for (i = 1; i <= count[filename]; i++) {
                    if (tolower(paths[filename][i]) ~ /\/torrents?\//) {
                        torrent_file = paths[filename][i];
                        break; # Found it, no need to search further for this group
                    }
                }

                # --- Output results for Bash processing ---
                if (torrent_file != "") {
                    print "DUPLICATE_GROUP_START"; # Marker for start of a processable group
                    print "PRIORITY_FILE:" torrent_file;

                    # Extract the mount point of the priority torrent file
                    match(torrent_file, /^\/mnt\/disk[0-9]+/, torrent_mount_arr);
                    torrent_mount = torrent_mount_arr[0];

                    for (i = 1; i <= count[filename]; i++) {
                        current_file = paths[filename][i];
                        if (current_file == torrent_file) {
                            continue; # Skip the priority file itself
                        }

                        # Extract the mount point of the current duplicate
                        match(current_file, /^\/mnt\/disk[0-9]+/, current_mount_arr);
                        current_mount = current_mount_arr[0];

                        # Mark for delete/hardlink ONLY if on a different mount point
                        if (current_mount != torrent_mount) {
                            print "DELETE_AND_HARDLINK:" current_file;
                        }
                    }
                    print "DUPLICATE_GROUP_END"; # Marker for end of a processable group
                } else {
                    # No torrent file found for this group of cross-disk duplicates
                    print "NO_TORRENT_PRIORITY_FILE_FOUND:";
                    for (i = 1; i <= count[filename]; i++) {
                        print "  " paths[filename][i];
                    }
                }
            }
        }
    }
' "$INPUT_SOURCE" | while IFS= read -r line; do
    if [[ "$line" == "DUPLICATE_GROUP_START" ]]; then
        CURRENT_GROUP_START="true"
        continue
    elif [[ "$line" == "DUPLICATE_GROUP_END" ]]; then
        CURRENT_GROUP_START="false"
        continue
    fi

    if [[ "$line" == "PRIORITY_FILE:"* ]]; then
        PRIORITY_FILE="${line#PRIORITY_FILE:}"
        log_message INFO "Found priority file for group: '$PRIORITY_FILE'"
    elif [[ "$line" == "DELETE_AND_HARDLINK:"* ]]; then
        FILE_TO_DELETE="${line#DELETE_AND_HARDLINK:}"
        log_message INFO "Processing cross-disk duplicate for deletion and hardlink: '$FILE_TO_DELETE'"

        if [[ -f "$FILE_TO_DELETE" ]]; then
            # --- START Hardlink Pre-checks (moved before deletion) ---

            # Extract the root /mnt/diskX of the torrent file
            if [[ "$PRIORITY_FILE" =~ ^(\/mnt\/disk[0-9]+)\/ ]]; then
                TORRENT_MOUNT="${BASH_REMATCH[1]}"
            else
                log_message ERROR "    Could not extract torrent mount point from '$PRIORITY_FILE'. Aborting delete and hardlink for '$FILE_TO_DELETE'."
                continue # Skip to the next iteration of the loop
            fi

            # Extract the root /mnt/diskX of the file to be deleted
            if [[ "$FILE_TO_DELETE" =~ ^(\/mnt\/disk[0-9]+)\/ ]]; then
                DELETED_FILE_MOUNT="${BASH_REMATCH[1]}"
            else
                log_message ERROR "    Could not extract mount point from '$FILE_TO_DELETE'. Aborting delete and hardlink for '$FILE_TO_DELETE'."
                continue # Skip to the next iteration of the loop
            fi

            # Calculate the relative path of the deleted file from its mount point
            RELATIVE_PATH_FULL="${FILE_TO_DELETE#"$DELETED_FILE_MOUNT"}"
            RELATIVE_DIR=$(dirname "$RELATIVE_PATH_FULL")

            # Construct the full path for the new hardlink on the torrent's disk
            NEW_HARDLINK_TARGET_DIR="$TORRENT_MOUNT$RELATIVE_DIR"
            NEW_HARDLINK_PATH="$NEW_HARDLINK_TARGET_DIR/$(basename "$FILE_TO_DELETE")"

            # Create directories if they don't exist
            if [[ ! -d "$NEW_HARDLINK_TARGET_DIR" ]]; then
                log_message INFO "  Target directory for hardlink does not exist: '$NEW_HARDLINK_TARGET_DIR'. Creating..."
                if [[ "$DRY_RUN" == "true" ]]; then # DRY RUN: mkdir
                    log_message DEBUG "  DRY RUN: Would create directory: mkdir -p \"$NEW_HARDLINK_TARGET_DIR\""
                else
                    mkdir -p "$NEW_HARDLINK_TARGET_DIR"
                fi
                if [[ "$DRY_RUN" == "false" && $? -ne 0 ]]; then # Check for real mkdir failure
                    log_message ERROR "    Failed to create directory '$NEW_HARDLINK_TARGET_DIR'. Aborting delete and hardlink for '$FILE_TO_DELETE'."
                    continue # Skip to the next iteration if directory creation fails
                fi
            fi

            # Check for overlapping name conflict
            if [[ -e "$NEW_HARDLINK_PATH" ]]; then
                log_message WARNING "    Name conflict detected at hardlink target path: '$NEW_HARDLINK_PATH'. A file or directory already exists there. Aborting delete and hardlink for '$FILE_TO_DELETE'."
                continue # Skip to the next iteration, do not create hardlink
            fi

            # --- END Hardlink Pre-checks ---

            # If all pre-checks passed, now proceed with deletion and hardlink
            log_message INFO "  Pre-checks passed. Proceeding with deletion and hardlink for '$FILE_TO_DELETE'."
            log_message INFO "  Deleting: '$FILE_TO_DELETE'"
            if [[ "$DRY_RUN" == "true" ]]; then # DRY RUN: rm
                log_message DEBUG "  DRY RUN: Would delete: rm -f \"$FILE_TO_DELETE\""
            else
                rm -f "$FILE_TO_DELETE"
            fi
            
            # Check for real rm failure or proceed with simulated success in dry run
            if [[ "$DRY_RUN" == "true" || $? -eq 0 ]]; then
                log_message INFO "    Successfully deleted '$FILE_TO_DELETE'."

                log_message INFO "  Creating hardlink: ln \"$PRIORITY_FILE\" \"$NEW_HARDLINK_PATH\""
                if [[ "$DRY_RUN" == "true" ]]; then # DRY RUN: ln
                    log_message DEBUG "  DRY RUN: Would create hardlink: ln \"$PRIORITY_FILE\" \"$NEW_HARDLINK_PATH\""
                else
                    ln "$PRIORITY_FILE" "$NEW_HARDLINK_PATH"
                fi

                if [[ "$DRY_RUN" == "true" || $? -eq 0 ]]; then # Check for real ln failure
                    log_message INFO "    Successfully created hardlink for '$(basename "$FILE_TO_DELETE")' to '$PRIORITY_FILE' at '$NEW_HARDLINK_PATH'."
                else
                    log_message ERROR "    Failed to create hardlink for '$(basename "$FILE_TO_DELETE")' to '$PRIORITY_FILE' at '$NEW_HARDLINK_PATH'."
                fi
            else # Actual rm failed
                log_message ERROR "    Failed to delete '$FILE_TO_DELETE'. Hardlink not created."
            fi
        else
            log_message WARNING "  File to delete ('$FILE_TO_DELETE') not found. Skipping."
        fi
    elif [[ "$line" == "NO_TORRENT_PRIORITY_FILE_FOUND:"* ]]; then
        log_message WARNING "No torrent priority file found for the following duplicates (but they span multiple disks):"
    else
        # Log other informational lines from awk, like specific duplicate files when no torrent is found
        log_message INFO "$line" # Default to INFO for these lines, can be changed to DEBUG if too verbose
    fi
done

# Cleanup of old logs is handled by the trap (TEMP_FILE is deleted if it was created)
log_message INFO "--- Script Finished ---"
