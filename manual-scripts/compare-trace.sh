#!/bin/bash
set -euo pipefail
[[ "${DEBUG:-false}" == "true" ]] && set -x

if [ "$#" -ne 2 ]; then
    echo 1>&2 "Usage: $0 <folder1> <folder2>"
    exit 1
fi
src_dir=$(dirname "$0")
folderOriginal="$1"
folderToCompare="$2"
identical=()
differents=()
notfoundinfolderToCompare=()
notfoundinfolder=()

# Count the total number of folder
total_folders=$(ls -l "$folderOriginal" | grep -c ^d)  

# Initialize a counter for the progress bar
count=0

# Loop through each folder in the input directory
for folder in "$folderOriginal"/*; do
    count=$(($count+1))
    id=$(basename "$folder")
    echo -ne "Processing $id: $((count * 100 / total_folders))% ($count / $total_folders) \r"
    result=$("$src_dir/compare-trace-one-folder.sh" "$folderOriginal" "$folderToCompare" "$id" "false")
    case $result in
      "identical")
        identical+=($id)
      ;;
      "different")
        differents+=($id)
      ;;
      "notfoundinfolderToCompare")
        notfoundinfolderToCompare+=($id)
      ;;
      "notfoundinfolder")
        notfoundinfolder+=($id)
      ;;
      *)
        sleep 1
      ;;
    esac
done
echo
echo "Comparison complete."
echo "Identical : ${#identical[@]}"
echo "notfoundinfolder : ${#notfoundinfolder[@]}"
for id in "${notfoundinfolder[@]}"; do
  echo 1>&2 "Error: traces.json not found in $folderOriginal/$id/"
done
echo "notfoundinfolderToCompare : ${#notfoundinfolderToCompare[@]}"
for id in "${notfoundinfolderToCompare[@]}"; do
  echo 1>&2 "Error: traces.json not found in $folderToCompare/$id/ but exits in $folderOriginal/$id/traces.json."
done
echo "differents : ${#differents[@]}"
for id in "${differents[@]}"; do
  echo 1>&2 "Files in $folderOriginal/$id/traces.json and $folderToCompare/$id/traces.json are different: $folderToCompare/$id/diff.txt"
done
echo "total : $total_folders"