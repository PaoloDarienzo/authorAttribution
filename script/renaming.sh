#!/bin/bash

for filename in *.txt; do

    # Grab the first 30 lines with carriage returns removed (tr -d):
    firstlines=$(head -n 30 "$filename" | tr -d '\r')

    # Capture the title and author. Note that sed doesn't have case-insensitive
    # patterns, so use e.g. [Tt] to manually make them case-insensitive. Also, use
    # [[:blank:]]* to allow any number of spaces and/or tabs after the ":".
    title=$(echo "$firstlines" | sed -n 's/^.*[Tt][Ii][Tt][Ll][Ee]:[[:blank:]]*//p')
    if [ -z "$title" ]; then
        # >&2 redirect std output to stderr
        echo "Unable to find Title: in $filename; skipping" >&2
        continue
    fi

    author=$(echo "$firstlines" | sed -n 's/^.*[Aa][Uu][Tt][Hh][Oo][Rr]:[[:blank:]]*//p')
    if [ -z "$author" ]; then
        echo "Unable to find Author: in $filename; skipping" >&2
        continue
    fi
    #creating new name convention
    new_name="$author,___,$title.txt"

    #renaming the file
    # Note: the filenames here will contain spaces, so double-quoting is *critical*
    #mv --backup=numbered -i -f "$filename" "$new_name"

    #check if dir of that author exists;
    #if true, moves the file there, otherwise (first branch) creates the dir and
    #moves the file there
    if [ ! -d '$author' ]; then
      mkdir -p "$author"
      mv --backup=numbered -i -f "$filename" "$author"/"$new_name"
    else
      mv --backup=numbered -i -f "$filename" "$author"/"$new_name"
    fi

done
