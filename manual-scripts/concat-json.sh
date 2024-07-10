    #!/bin/bash
set -euo pipefail
[[ "${DEBUG:-false}" == "true" ]] && set -x

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
total_folders=$(ls -l "$1" | grep -c ^d)  

# Initialize a counter for the progress bar
count=0

for folder in "$1"/*; do 
    count=$(($count+1))
    if [ -d "$folder" ]; then
        dirname=$(basename "$folder")
        echo "Processing directory $dirname : $((count * 100 / total_folders))% ($count / $total_folders)"
        "$src_dir/concat-json-one-folder.sh" "$folder" "$2"
    fi
done
echo "Completed!"