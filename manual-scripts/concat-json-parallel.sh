#!/usr/bin/env bash
set -euo pipefail
[[ "${DEBUG:-false}" == "true" ]] && set -x

# Install GNU Parallel if not already installed
# sudo apt-get install parallel

# Check if input directory is provided
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <folder1> <folder2>"
    exit 1
fi
# Check if input directory exists
if [ ! -d "$1" ]; then
    echo "Error: Directory '$1' not found."
    exit 1
fi
src_dir=$(dirname "$0")
echo $src_dir

# Count the total number of folder
folders=$(find "$1" -mindepth 1 -maxdepth 1 -type d)
total_folders=$(echo "$folders" | wc -l)

# Initialize a counter for the progress bar
count=0

export src_dir  # Export src_dir so it can be used in the parallel call

# Run the script in parallel for each folder
parallel "$src_dir/concat-json-one-folder.sh" ::: "$1"/* ::: "$2"

echo "Completed!"
exit 0