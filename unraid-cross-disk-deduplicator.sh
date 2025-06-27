#!/bin/bash
set -euo pipefail
# This script identifies and manages duplicate files across specified disk mounts.
# It prioritizes files located in paths containing "torrent" or "torrents" and
# replaces other identical files on different disks with hardlinks to the
# prioritized version. This helps in deduplicating data and saving disk space,
# particularly useful for media collections managed by torrent clients.
#
# Usage:
#   sudo ./cross_disk_deduplicator.sh [-r|--real-run] [-d|--dry-run] [-p <input_file>] [-v <LOG_LEVEL>] [-e <exclude_dir> ...] [DISK_MOUNT_PATHS...]
#
# Options:
#   -r, --real-run       : Enable real run mode. Files will be deleted and hardlinks created.
#                          USE WITH CAUTION. Backups are highly recommended.
#   -d, --dry-run        : Enable dry run mode (default). No files will be deleted or
#                          hardlinks created. The script will only log what it *would* do.
#   -p <input_file>      : Process file paths from the specified input file instead
#                          of scanning disk mounts. The input file should contain
#                          one file path per line. Exclusions (-e) do not apply to
#                          files read from this input.
#   -v <LOG_LEVEL>       : Set the minimum log level for console and file output.
#                          Valid levels: DEBUG, INFO, WARNING, ERROR, CRITICAL.
#                          Default for console: INFO, Default for file: INFO.
#   -e <exclude_dir>     : Specify a directory to exclude from scanning. Can be used
#                          multiple times for multiple exclusions. Wildcards like
#                          /mnt/disk*/sffebooks are supported.
#
# Positional Arguments:
#   DISK_MOUNT_PATHS...  : One or more paths to disk mounts to scan (e.g., /mnt/disk1 /mnt/disk2).
#                          If not provided, the script defaults to scanning all /mnt/disk* paths.
#
# Prerequisites:
#   - Must be run with root privileges (sudo) to perform file deletions and
#     create hardlinks across different mount points.

# --- Configuration ---
LOG_DIR="./logs"
NUM_LOGS_TO_RETAIN=7 # Number of old log files to keep
# DISK_MOUNTS will be determined by arguments or defaulted in parse_args
TEMP_FILE="" # For checksummed paths (checksum<tab>filepath)
TEMP_FILE_PASS1_OUTPUT="" # For null-separated potential duplicates (filepath\0)
DRY_RUN="true" # Default to dry run
INPUT_FILE=""
EXCLUDE_DIRS=() # Array to hold directories to exclude
CHECKSUM_CMD="sha256sum" # Use sha256sum for checksumming. Can change to md5sum if preferred.

# --- Logging Functions ---
LOG_LEVELS=(DEBUG INFO WARNING ERROR CRITICAL)
declare -A LOG_LEVEL_MAP
for i in "${!LOG_LEVELS[@]}"; do
    LOG_LEVEL_MAP["${LOG_LEVELS[$i]}"]=$i
done

CONSOLE_LOG_LEVEL_THRESHOLD=${LOG_LEVEL_MAP["INFO"]}
FILE_LOG_LEVEL_THRESHOLD=${LOG_LEVEL_MAP["INFO"]}

SCRIPT_START_TIME=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="${LOG_DIR}/deduplicator_${SCRIPT_START_TIME}.log"

mkdir -p "$LOG_DIR"

log_message() {
    local level_name="$1"
    local message="$2"
    local level_value=${LOG_LEVEL_MAP["$level_name"]}

    if [[ "$level_value" -ge "$CONSOLE_LOG_LEVEL_THRESHOLD" ]]; then
        echo "$(date +"%Y-%m-%d %H:%M:%S") [${level_name}] ${message}" >&2
    fi

    if [[ "$level_value" -ge "$FILE_LOG_LEVEL_THRESHOLD" ]]; then
        echo "$(date +"%Y-%m-%d %H:%M:%S") [${level_name}] ${message}" >> "$LOG_FILE"
    fi
}

cleanup_old_logs() {
    if [[ -d "$LOG_DIR" ]]; then
        log_message INFO "Cleaning up old log files in $LOG_DIR..."
        find "$LOG_DIR" -maxdepth 1 -name "deduplicator_*.log" -type f \
            -printf '%T@ %p\n' | sort -nr | tail -n +$((NUM_LOGS_TO_RETAIN + 1)) |
            awk '{print $2}' | xargs -r rm -f
        log_message INFO "Finished log cleanup."
    fi
}

# --- Argument Parsing ---
parse_args() {
    local OPTIND
    while getopts "rdp:v:e:" opt; do
        case "$opt" in
            r) DRY_RUN="false" ;;
            d) DRY_RUN="true" ;;
            p) INPUT_FILE="$OPTARG" ;;
            v)
                local upper_optarg=$(echo "$OPTARG" | tr '[:lower:]' '[:upper:]')
                if [[ -v LOG_LEVEL_MAP["$upper_optarg"] ]]; then
                    CONSOLE_LOG_LEVEL_THRESHOLD=${LOG_LEVEL_MAP["$upper_optarg"]}
                    FILE_LOG_LEVEL_THRESHOLD=${LOG_LEVEL_MAP["$upper_optarg"]}
                else
                    log_message ERROR "Invalid log level: $OPTARG. Valid levels: ${LOG_LEVELS[*]}"
                    exit 1
                fi
                ;;
            e)
                EXCLUDE_DIRS+=("$OPTARG")
                ;;
            \?)
                echo "Invalid option: -$OPTARG" >&2
                exit 1
                ;;
            :)
                echo "Option -$OPTARG requires an argument." >&2
                exit 1
                ;;
        esac
    done
    shift $((OPTIND - 1)) # Remove parsed options from arguments

    # Remaining arguments are treated as DISK_MOUNTS
    if [[ "$#" -gt 0 ]]; then
        DISK_MOUNTS=("$@")
        log_message INFO "Scanning specified disk mounts: ${DISK_MOUNTS[*]}"
    else
        # Default to all /mnt/disk*, then filter out /mnt/disks
        local -a all_disks=(/mnt/disk*)
        local -a filtered_disks=()
        for disk_path in "${all_disks[@]}"; do
            if [[ "$disk_path" != "/mnt/disks" ]]; then
                filtered_disks+=("$disk_path")
            fi
        done
        DISK_MOUNTS=("${filtered_disks[@]}")
        log_message INFO "No specific disk mounts provided. Defaulting to scanning all /mnt/disk* (excluding /mnt/disks): ${DISK_MOUNTS[*]}."
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        log_message INFO "Running in DRY RUN mode. No changes will be made to files."
    else
        log_message CRITICAL "Running in REAL RUN mode. Files WILL BE DELETED and hardlinks created. Backups HIGHLY recommended!"
        # Add an extra pause for confirmation in real run mode, if desired.
        # read -p "Are you sure you want to proceed? (y/N): " -n 1 -r
        # echo
        # if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        #     exit 1
        # fi
    fi
}

# --- Core Logic Functions ---

# Function to build exclusion arguments for the find command as a Bash array.
# It takes the current disk mount path as an argument to correctly resolve wildcards.
# This version explicitly avoids adding backslashes to parentheses; the calling function will handle that.
build_find_exclude_args_array() {
    local current_disk_mount="$1" # The specific disk path being searched (e.g., /mnt/disk1)
    local -a exclude_args_raw=() # This array will hold the raw arguments, including "(" and ")"

    local -a active_disk_exclusions=() # Store only the exclusions relevant to this specific disk

    for exclude_pattern in "${EXCLUDE_DIRS[@]}"; do # e.g., "/mnt/user/disk*/sffebooks"
        local resolved_exclude_path=""

        # Logic to transform the exclusion pattern to a disk-specific path
        # 1. Handle /mnt/user/disk*/... patterns (Unraid user share style)
        if [[ "$exclude_pattern" =~ ^/mnt/user/disk\*/(.*)$ ]]; then
            # Extract the part after /mnt/user/disk* (e.g., "sffebooks")
            local suffix="${BASH_REMATCH[1]}"
            resolved_exclude_path="${current_disk_mount}/${suffix}"
            log_message DEBUG "  Transformed /mnt/user/disk* pattern: '$exclude_pattern' -> '$resolved_exclude_path'"
        # 2. Handle /mnt/disk*/... patterns (direct disk-level wildcards)
        elif [[ "$exclude_pattern" =~ ^/mnt/disk\*/(.*)$ ]]; then
            # Extract the part after /mnt/disk*
            local suffix="${BASH_REMATCH[1]}"
            resolved_exclude_path="${current_disk_mount}/${suffix}"
            log_message DEBUG "  Transformed /mnt/disk* pattern: '$exclude_pattern' -> '$resolved_exclude_path'"
        # 3. Handle absolute paths that might be under the current disk (e.g., /mnt/disk1/mydata)
        elif [[ "$exclude_pattern" == "$current_disk_mount"* ]]; then
            resolved_exclude_path="$exclude_pattern"
            log_message DEBUG "  Using direct disk-specific path: '$resolved_exclude_path'"
        fi

        if [[ -n "$resolved_exclude_path" ]]; then
            # Crucial check: Ensure the resolved path actually exists as a directory to exclude
            if [[ -d "$resolved_exclude_path" ]]; then
                active_disk_exclusions+=("$resolved_exclude_path")
                log_message DEBUG "Adding disk-specific exclusion for '$current_disk_mount': '$resolved_exclude_path'"
            else
                log_message DEBUG "Resolved exclusion path '$resolved_exclude_path' for disk '$current_disk_mount' does not exist as a directory. Skipping."
            fi
        fi
    done

    # The rest of the function (building exclude_args_raw from active_disk_exclusions) remains the same
    if [[ ${#active_disk_exclusions[@]} -gt 0 ]]; then
        # If there are multiple active exclusions, wrap them in logical parentheses for find
        if [[ ${#active_disk_exclusions[@]} -gt 1 ]]; then
            exclude_args_raw+=("(") # Add literal (
        fi

        local first_exclude=true
        for path_to_exclude in "${active_disk_exclusions[@]}"; do
            if [[ "$first_exclude" == "false" ]]; then
                exclude_args_raw+=("-o") # Add -o for subsequent exclusions
            fi
            first_exclude=false
            exclude_args_raw+=("-path" "$path_to_exclude" "-prune")
        done

        if [[ ${#active_disk_exclusions[@]} -gt 1 ]]; then
            exclude_args_raw+=(")") # Add literal )
        fi
        # Add the final -o to logically connect with the -type f -print0 part
        exclude_args_raw+=("-o")
    fi
    # Print each argument on a new line, so read -r can correctly separate them.
    # The actual escaping for find's special characters will happen in gather_files_from_disks.
    printf '%s\n' "${exclude_args_raw[@]}"
}

# New function to just collect raw files from disks
_collect_raw_files_from_disks() {
    local -n _all_files_array=$1 # Nameref to the array in the caller's scope
    local _disk_mounts=("${@:2}") # Capture remaining args as disk mounts

    _all_files_array=() # Clear the array for fresh collection

    for disk in "${_disk_mounts[@]}"; do
        if [[ -d "$disk" ]]; then
            log_message INFO "Searching in '$disk'..."

            local -a find_exclude_args_raw=()
            while IFS= read -r line; do
                find_exclude_args_raw+=("$line")
            done < <(build_find_exclude_args_array "$disk")

            local -a final_find_cmd_args=("$disk")
            for arg in "${find_exclude_args_raw[@]}"; do
                if [[ "$arg" == "(" ]]; then
                    final_find_cmd_args+=("\(")
                elif [[ "$arg" == ")" ]]; then
                    final_find_cmd_args+=("\)")
                else
                    final_find_cmd_args+=("$arg")
                fi
            done
            final_find_cmd_args+=("-type" "f" "-print0")

            log_message DEBUG "Executing find command for disk '$disk': find ${final_find_cmd_args[*]}"

            local -a current_disk_files_array=()
            if ! mapfile -d '' -t current_disk_files_array < <(find "${final_find_cmd_args[@]}" 2>> "$LOG_FILE"); then
                log_message WARNING "Errors encountered during find scan of '$disk'. Check log file for details."
                continue
            fi
            _all_files_array+=( "${current_disk_files_array[@]}" )
        else
            log_message WARNING "Disk mount '$disk' does not exist or is not a directory. Skipping."
        fi
    done

    if [[ ${#_all_files_array[@]} -eq 0 ]]; then
        log_message WARNING "No files found on specified disk mounts or after exclusions during raw file collection."
        return 1
    fi
    return 0
}


gather_files_from_disks() {
    log_message INFO "Beginning file gathering and checksumming process."
    TEMP_FILE=$(mktemp /tmp/dedup_paths.XXXXXXXXXX) # For checksummed paths (checksum<tab>filepath)
    TEMP_FILE_PASS1_OUTPUT=$(mktemp /tmp/dedup_pass1_output.XXXXXXXXXX) # For null-separated potential duplicates (filepath\0)
    log_message DEBUG "Temporary file for checksums (final output): $TEMP_FILE"
    log_message DEBUG "Temporary file for Pass 1 AWK output: $TEMP_FILE_PASS1_OUTPUT"

    # Clear previous content of the ephemeral temporary files (it should be empty, but for safety)
    > "$TEMP_FILE"
    > "$TEMP_FILE_PASS1_OUTPUT"

    local -a all_files_array=()
    if ! _collect_raw_files_from_disks all_files_array "${DISK_MOUNTS[@]}"; then
        log_message INFO "No raw files collected. Exiting gather_files_from_disks."
        return 1
    fi
    log_message INFO "Initial file gathering complete. Now identifying name-based cross-disk duplicates..."

    # --- Pass 1: Identify potential duplicates by name and disk ---
    # Pipe all null-separated file paths from the array to AWK_PASS1 to filter for name-based, cross-disk duplicates.
    # AWK_PASS1 will output null-separated paths of only these potential duplicates to TEMP_FILE_PASS1_OUTPUT.
    printf "%s\0" "${all_files_array[@]}" | \
    awk -v RS='\0' -v FS='/' '
        {
            if (length($0) == 0) next;
            filepath = $0;
            # Get basename (last field with FS="/")
            n = split(filepath, parts, "/");
            basename = parts[n];

            # Extract mount point
            # Use gensub to extract /mnt/diskN reliably
            match(filepath, /^\/mnt\/disk[0-9]+/, mount_arr);
            mount_point = (mount_arr[0] != "") ? mount_arr[0] : "UNKNOWN_MOUNT_NON_STANDARD";

            # Store filepaths by basename and track unique mount points for each basename
            filepaths_for_basename[basename][++count_filepaths_for_basename[basename]] = filepath;
            unique_mounts_for_basename[basename][mount_point] = 1;
        }
        END {
            for (bname in filepaths_for_basename) {
                # Count unique mounts for this basename
                num_unique_mounts = 0;
                for (mnt in unique_mounts_for_basename[bname]) {
                    num_unique_mounts++;
                }

                if (num_unique_mounts > 1) { # This basename has files on multiple disks
                    # Output all filepaths for this basename, null-separated
                    for (i = 1; i <= count_filepaths_for_basename[bname]; i++) {
                        printf "%s\0", filepaths_for_basename[bname][i];
                    }
                }
            }
        }' > "$TEMP_FILE_PASS1_OUTPUT" # Redirect AWK output to new temporary file

    if [[ $? -ne 0 ]]; then
        log_message WARNING "Errors encountered during name-based duplicate identification (AWK Pass 1). Check log file for details."
        return 1 # Indicate failure if AWK fails
    fi

    if [[ ! -s "$TEMP_FILE_PASS1_OUTPUT" ]]; then
        log_message INFO "No potential name-based cross-disk duplicates found. No checksums will be calculated."
        return 1 # Exit cleanly as nothing to process further
    fi

    log_message INFO "Identified potential name-based cross-disk duplicates. Calculating checksums for these files..."
    # --- Pass 2: Calculate checksums only for potential duplicates ---
    # Pipe null-separated potential duplicates from TEMP_FILE_PASS1_OUTPUT to xargs and sha256sum.
    # The output of sha256sum (checksum<space><space>filepath) is then piped to a small awk
    # which formats it to "checksum<tab>filepath" for TEMP_FILE.
    log_message DEBUG "Sending files for checksum calculation via '$CHECKSUM_CMD' from '$TEMP_FILE_PASS1_OUTPUT'..."
    xargs -0 "$CHECKSUM_CMD" 2>> "$LOG_FILE" < "$TEMP_FILE_PASS1_OUTPUT" | \
    awk '
    {
        checksum = $1;
        # Get filepath, which can contain spaces, by taking everything after the first field ($1) and the two spaces
        filepath = substr($0, length(checksum) + 3); 
        printf "%s\t%s\n", checksum, filepath;
    }' > "$TEMP_FILE" # Redirect the awk output for main processing to TEMP_FILE
    
    if [[ $? -ne 0 ]]; then
        log_message WARNING "Errors encountered during checksum generation for potential duplicates. Check log file for details."
        return 1 # Indicate failure if checksum generation fails
    fi

    if [[ ! -s "$TEMP_FILE" ]]; then
        log_message WARNING "Checksums file is empty after processing potential duplicates. This might mean no actual checksums were generated."
        return 1
    fi

    # Post-checksum logging from TEMP_FILE
    log_message DEBUG "Logging calculated checksums from temporary file: $TEMP_FILE"
    while IFS=$'\t' read -r checksum filepath; do
        log_message DEBUG "Calculated checksum: $checksum for file: $filepath"
    done < "$TEMP_FILE"

    log_message INFO "Finished gathering file paths and checksums for potential duplicates."
    return 0
}

process_input_file() {
    log_message INFO "Processing file paths from input file: $INPUT_FILE"
    TEMP_FILE=$(mktemp /tmp/dedup_paths.XXXXXXXXXX)
    log_message DEBUG "Temporary file for paths: $TEMP_FILE"

    if [[ ! -f "$INPUT_FILE" ]]; then
        log_message ERROR "Input file '$INPUT_FILE' not found."
        return 1
    fi

    log_message DEBUG "Calculating checksums for files from input file '$INPUT_FILE'..."
    # Read line by line, trim whitespace, filter empty lines, and calculate checksums.
    # Output to TEMP_FILE in the format: CHECKSUM<tab>FILEPATH
    while IFS= read -r line; do
        if [[ -n "${line// }" ]]; then # Check if line is not empty or just whitespace
            # Calculate checksum for each file from input and format it
            if [[ -f "$line" ]]; then
                "$CHECKSUM_CMD" "$line" 2>> "$LOG_FILE" | awk '
                {
                    checksum = $1;
                    filepath = substr($0, length(checksum) + 3); # Get filepath after checksum and two spaces
                    printf "%s\t%s\n", checksum, filepath;
                }' >> "$TEMP_FILE"
            else
                log_message WARNING "File from input list not found: '$line'. Skipping checksum."
            fi
        fi
    done < "$INPUT_FILE"

    if [[ ! -s "$TEMP_FILE" ]]; then
        log_message WARNING "No valid file paths found in input file '$INPUT_FILE' or no checksums generated."
        return 1
    fi

    # Post-checksum logging for input file processing
    log_message DEBUG "Logging calculated checksums from temporary file (input): $TEMP_FILE"
    while IFS=$'\t' read -r checksum filepath; do
        log_message DEBUG "Calculated checksum: $checksum for file: $filepath"
    done < "$TEMP_FILE"

    log_message INFO "Finished processing input file and checksums."
    return 0
}


# AWK script (Pass 2) to identify cross-disk duplicates based on checksum and prioritize torrent files.
# Input: Newline-separated records, with tab (\t) as field separator (CHECKSUM<tab>FILEPATH).
#        These are already filtered to be name-based, cross-disk duplicates from the first pass.
# Output: Newline-separated records for Bash, with \x1f (ASCII Unit Separator) as internal field separator
#         for DELETE_AND_HARDLINK records.
#
# Output format examples:
# DUPLICATE_GROUP_START
# PRIORITY_FILE:/path/to/torrent/file
# DELETE_AND_HARDLINK:/path/to/duplicate/file_to_delete<US>/path/to/new_hardlink_target_dir
# NO_TORRENT_PRIORITY_FILE_FOUND
# /path/to/file_without_torrent_priority_duplicate
# /path/to/another_file_without_torrent_priority_duplicate
# (Where <US> is ASCII Unit Separator, \x1f)
AWK_SCRIPT='
BEGIN {
    FS = "\t"; # Input field separator is tab (CHECKSUM<tab>FILEPATH)
    RS = "\n"; # Input record separator is newline
}
{
    if (length($0) == 0) { # Skip empty lines
        next
    }
    checksum = $1;
    file_path = $2; # Full path, correctly captured due to FS="\t"

    # Extract mount point (e.g., /mnt/disk1 or /mnt/disk2)
    match(file_path, /^\/mnt\/disk[0-9]+/, mount_arr);
    # Ensure mount_arr[0] exists before using it
    mount_point = (mount_arr[0] != "") ? mount_arr[0] : "UNKNOWN_MOUNT"; # Handle cases where path doesn''t match /mnt/diskN

    # Store file paths and their mount points, grouped by checksum
    # These are already "potential duplicates" by name and disk from Pass 1,
    # now we are confirming by content.
    files_by_checksum[checksum][file_path] = mount_point;
    # Keep a count of files per checksum to know if there are duplicates
    count_by_checksum[checksum]++;
}
END {
    # Iterate through each unique checksum
    for (checksum in files_by_checksum) {
        if (count_by_checksum[checksum] > 1) { # Found content duplicates for this checksum
            # Collect all paths for this specific checksum
            split("", current_paths_for_checksum_array); # Clear array for current checksum group
            path_idx = 0;
            for (fp_current in files_by_checksum[checksum]) {
                current_paths_for_checksum_array[++path_idx] = fp_current;
            }

            # We know these files are identical by content and were already filtered as name-based, cross-disk duplicates.
            # Now, apply torrent prioritization to decide which to keep.
            priority_file = "";
            for (i = 1; i <= path_idx; i++) {
                current_file = current_paths_for_checksum_array[i];
                # Case-insensitive match for "torrent" or "torrents" in the path
                if (tolower(current_file) ~ /\/torrents?\//) {
                    priority_file = current_file;
                    break;
                }
            }

            if (priority_file != "") {
                print "DUPLICATE_GROUP_START";
                printf "PRIORITY_FILE:%s\n", priority_file;
                for (i = 1; i <= path_idx; i++) {
                    current_file = current_paths_for_checksum_array[i];
                    if (current_file != priority_file) {
                        # Double-check that it''s on a different disk than the priority file, for robustness.
                        if (files_by_checksum[checksum][current_file] != files_by_checksum[checksum][priority_file]) {
                            split(current_file, current_path_parts, "/");
                            filename = current_path_parts[length(current_path_parts)];

                            original_mount_path_len = length(files_by_checksum[checksum][current_file]);
                            relative_path = substr(current_file, original_mount_path_len + 1);

                            new_hardlink_target_dir = files_by_checksum[checksum][priority_file] relative_path;
                            sub("/" filename "$", "", new_hardlink_target_dir); # Remove filename to get only dir

                            # Using \x1f (ASCII Unit Separator) as internal field separator
                            printf "DELETE_AND_HARDLINK:%s\x1f%s\n", current_file, new_hardlink_target_dir;
                        }
                    }
                }
            } else {
                # No torrent priority file found within this content-duplicate group spanning multiple disks
                print "NO_TORRENT_PRIORITY_FILE_FOUND";
                for (i = 1; i <= path_idx; i++) {
                    print current_paths_for_checksum_array[i];
                }
            }
        }
    }
}
'

process_duplicates() {
    log_message INFO "Starting duplicate processing..."
    # Read newline-separated records from awk output.
    awk -f <(echo "$AWK_SCRIPT") "$TEMP_FILE" | while IFS= read -r line; do
        if [[ "$line" == "DUPLICATE_GROUP_START" ]]; then
            log_message DEBUG "Processing new duplicate group."
        elif [[ "$line" == "PRIORITY_FILE:"* ]]; then
            PRIORITY_FILE="${line#PRIORITY_FILE:}" # Remove prefix (no space after colon)
            log_message INFO "  Priority file found: '$PRIORITY_FILE'"
        elif [[ "$line" == "DELETE_AND_HARDLINK:"* ]]; then
            local rest="${line#DELETE_AND_HARDLINK:}" # Remove prefix
            # Read fields separated by ASCII Unit Separator (\x1f)
            IFS=$'\x1f' read -r FILE_TO_DELETE NEW_HARDLINK_TARGET_DIR <<< "$rest"

            log_message INFO "  Candidate for deduplication:"
            log_message INFO "    File to delete: '$FILE_TO_DELETE'"
            log_message INFO "    New hardlink directory: '$NEW_HARDLINK_TARGET_DIR'"

            if [[ -f "$FILE_TO_DELETE" ]]; then
                log_message DEBUG "    File exists: '$FILE_TO_DELETE'"

                # Construct the full path for the new hardlink
                local NEW_HARDLINK_PATH="${NEW_HARDLINK_TARGET_DIR}/$(basename "$FILE_TO_DELETE")"

                # Check if the target hardlink path already exists or is a directory
                if [[ -e "$NEW_HARDLINK_PATH" ]]; then
                    if [[ -f "$NEW_HARDLINK_PATH" && "$(realpath "$NEW_HARDLINK_PATH")" == "$(realpath "$PRIORITY_FILE")" ]]; then
                        log_message INFO "    Hardlink already exists and points to priority file. Skipping."
                        continue
                    else
                        log_message ERROR "    Target hardlink path already exists and is not the same hardlink: '$NEW_HARDLINK_PATH'. Skipping deletion and hardlink creation to prevent overwrite."
                        continue
                    fi
                fi

                # Create the target directory if it doesn't exist
                if [[ ! -d "$NEW_HARDLINK_TARGET_DIR" ]]; then
                    log_message INFO "    Target directory for hardlink does not exist. Creating: '$NEW_HARDLINK_TARGET_DIR'"
                    if [[ "$DRY_RUN" == "true" ]]; then
                        log_message DEBUG "      DRY RUN: mkdir -p \"$NEW_HARDLINK_TARGET_DIR\""
                    else
                        mkdir -p "$NEW_HARDLINK_TARGET_DIR"
                    fi

                    if [[ "$DRY_RUN" == "false" && $? -ne 0 ]]; then
                        log_message ERROR "      Failed to create directory: '$NEW_HARDLINK_TARGET_DIR'. Skipping hardlink."
                        continue
                    fi
                fi

                # Delete the original file
                log_message INFO "    Attempting to delete: '$FILE_TO_DELETE'"
                if [[ "$DRY_RUN" == "true" ]]; then
                    log_message DEBUG "      DRY RUN: rm -f \"$FILE_TO_DELETE\""
                else
                    rm -f "$FILE_TO_DELETE"
                fi

                if [[ "$DRY_RUN" == "true" || $? -eq 0 ]]; then # Check for real rm failure
                    log_message INFO "    Successfully deleted: '$FILE_TO_DELETE'"

                    # Create hardlink
                    log_message INFO "    Attempting to create hardlink from '$PRIORITY_FILE' to '$NEW_HARDLINK_PATH'"
                    if [[ "$DRY_RUN" == "true" ]]; then # DRY RUN: ln
                        log_message DEBUG "      DRY RUN: Would create hardlink: ln \"$PRIORITY_FILE\" \"$NEW_HARDLINK_PATH\""
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
        elif [[ "$line" == "NO_TORRENT_PRIORITY_FILE_FOUND" ]]; then
            log_message WARNING "No torrent priority file found for the following duplicates (but they span multiple disks):"
        else
            # This catch-all logs any other line from AWK (these would be the file paths in the no-torrent group)
            log_message INFO "$line"
        fi
    done
    log_message INFO "Finished duplicate processing."
}

# --- Main Execution ---
main() {
    # Trap to ensure temporary files are deleted on exit
    trap 'if [[ -n "$TEMP_FILE" && -f "$TEMP_FILE" ]]; then rm -f "$TEMP_FILE"; log_message DEBUG "Cleaned up temporary file: $TEMP_FILE"; fi; if [[ -n "$TEMP_FILE_PASS1_OUTPUT" && -f "$TEMP_FILE_PASS1_OUTPUT" ]]; then rm -f "$TEMP_FILE_PASS1_OUTPUT"; log_message DEBUG "Cleaned up temporary file: $TEMP_FILE_PASS1_OUTPUT"; fi; cleanup_old_logs' EXIT

    parse_args "$@" # Pass all original arguments to parsing

    # If INPUT_FILE is specified, it overrides scanning disk mounts.
    if [[ -n "$INPUT_FILE" ]]; then
        if ! process_input_file; then
            log_message ERROR "Failed to process input file. Exiting."
            exit 1
        fi
    else
        # Proceed with gathering from disk mounts only if no input file is specified.
        if [[ "$EUID" -ne 0 ]]; then
            log_message CRITICAL "This script must be run with root privileges to scan disk mounts. Please use sudo."
            exit 1
        fi
        if ! gather_files_from_disks; then
            log_message INFO "No files to process or an error occurred during gathering. Exiting."
            exit 0 # Exit cleanly if no files found
        fi
    fi

    process_duplicates

    log_message INFO "--- Script Finished ---"
}

main "$@"
