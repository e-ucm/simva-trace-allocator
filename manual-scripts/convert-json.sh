j#!/usr/bin/env bash
set -euo pipefail
[[ "${DEBUG:-false}" == "true" ]] && set -x

# Check if input directory is provided
if [ "$#" -ne 3 ]; then
    echo 1>&2 "Usage: $0 <file1> <file2> <mode : one-per-line-to-array or array-to-one-per-line>"
    exit 1
fi

# Check if input directory exists
if [ ! -f "$1" ]; then
    echo 1>&2  "Error: File '$1' not found."
    exit 1
fi

# Extract folder name from the provided path
input_file="$1"
output_file="$2"
mode="$3"

# Validate mode
if [[ "$mode" != "one-per-line-to-array" && "$mode" != "array-to-one-per-line" ]]; then
    echo "Invalid mode! Use either 'one-per-line-to-array' or 'array-to-one-per-line'."
    exit 1
fi;

# Conversion functions
convert_one_per_line_to_array() {
    jq -s '.' "$input_file" > "$output_file"
}

convert_array_to_one_per_line() {
    jq -c '.[]' "$input_file" > "$output_file"
}

# Perform conversion based on mode
if [[ "$mode" == "one-per-line-to-array" ]]; then
    convert_one_per_line_to_array
elif [[ "$mode" == "array-to-one-per-line" ]]; then
    convert_array_to_one_per_line
fi

# Check if the conversion was successful
if [[ $? -eq 0 ]]; then
    echo "Conversion successful! Output saved to $output_file"
else
    echo "Conversion failed!"
    exit 1
fi
exit 0