#!/usr/bin/env bash
set -euo pipefail
[[ "${DEBUG:-false}" == "true" ]] && set -x

# Check if input directory is provided
if [ "$#" -ne 2 ]; then
    echo 1>&2 "Usage: $0 <folder1> <folder2>"
    exit 1
fi
# Check if input directory exists
if [ ! -d "$1" ]; then
    echo 1>&2  "Error: Directory '$1' not found."
    exit 1
fi

# Extract folder name from the provided path
folder=$1
output_folder="$2"

# Extract folder name from the provided path
folder_name=$(basename "$folder")

# Create the output folder if it does not exist
if [[ ! -d "$output_folder" ]]; then 
    mkdir -p "$output_folder"
fi
# Extract the ID from the folder name (assuming it's after an underscore)
folder_id="${folder_name#*=}"
output_dir="$output_folder/$folder_id"
# Create the output directory if it does not exist
if [[ ! -d "$output_dir" ]]; then
    mkdir -p "$output_dir"
fi
output_file="$output_dir/traces.json"

# Concatenate files only if the output file doesn't exist
if [[ ! -e "$output_file" ]]; then 
    files=$(find $folder -mindepth 1 -maxdepth 1 -iname \*.json -type f)
    total_files=$(echo "$files" | wc -w)  # Count the total number of files
    
    # Initialize a counter for the progress bar
    count=0

    # Loop through each file in the input directory
    for file in $files; do
        count=$(($count+1))
        filename=$(basename -- "$file")
        #echo "Processing $filename..."
        cat "$file" >> "$output_file"

        # Update the progress bar
        echo -ne "Progress: $((count * 100 / total_files))% ($count / $total_files) \r"
    done
    
    # Print a newline after the loop completes
    echo
    
    echo "Concatenation completed. Output saved in $output_file."
else 
    echo "Concatenation file already present in $output_file."
fi
exit 0