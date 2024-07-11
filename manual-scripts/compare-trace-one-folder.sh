#!/usr/bin/env bash
set -euo pipefail
[[ "${DEBUG:-false}" == "true" ]] && set -x

if [ "$#" -lt 3 ] || [ "$#" -gt 4 ]; then
    echo 1>&2 "Usage: $0 <folder1> <folder2> <id> [<outputError : true or false>]"
    exit 1
fi

folder1="$1"
folder2="$2"
id="$3"
outputError="true"
if [ "$#" -eq 4 ]; then
    outputError=$4
fi
#Error in exec
result=1
if [ ! -f "$folder1/$id/traces.json" ]; then
    if [ -f "$folder2/$id/traces.json" ]; then
        # checking File size = 0
        if [[ ! -s "$folder2/$id/traces.json" ]]; then
            #identical
            result=0
        else 
            if [[ $outputError == "true" ]]; then 
                echo 1>&2 "Error: traces.json not found in $folder1/$id/"
            fi
            #notfoundinfolder
            result=4
        fi
    else
        if [[ $outputError == "true" ]]; then 
            echo 1>&2 "Error: traces.json not found in $folder1/$id/"
        fi
        #notfoundinfolder
        result=4
    fi
else
    # Check if traces.json exists in the corresponding subfolder in the other folder
    if [ ! -f "$folder2/$id/traces.json" ]; then
        # checking File size = 0
        if [[ -f "$folder1/$id/traces.json" ]] && [[ ! -s "$folder1/$id/traces.json" ]]; then
            #identical
            result=0
        else 
            if [[ $outputError == "true" ]]; then 
                echo 1>&2 "Error: traces.json not found in $folder2/$id/ but exits in $folder1/$id/traces.json."
            fi
            #notfoundinfolderToCompare
            result=5
        fi
    else
        # Compare contents of traces.json files and show differences
        diffoutput=$(diff -w -B "$folder1/$id/traces.json" "$folder2/$id/traces.json")
        if [[ $diffoutput == "" ]]; then
            #identical
            result=0
        else
            #different
            result=3
            $diffoutput > "$folder2/$id/diff.txt"
            if [[ $outputError == "true" ]]; then 
                echo 1>&2 "Files in $folder1/$id/traces.json and $folder2/$id/traces.json are different: $folder2/$id/diff.txt"
            fi
        fi
    fi
fi
exit $result