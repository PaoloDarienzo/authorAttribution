#!/bin/bash

read -p "Are you sure to launch the script (y/n)? " -n 1 -r
echo    #move to a new line
if [[ $REPLY =~ ^[Yy]$ ]]
then
    # do dangerous stuff

    #extracting txt file(s) from directories
	for dir in *; do
		#if the file is a directory
		if [ -d "$dir" ]; then
		  #move up by one the file(s)
		  mv "$dir"/*.txt .
		  #deleting directory (with content non-txt)
		  rm -r "$dir"
		fi
	done

    mkdir -p "00000_AUTHOR_NOT_FOUND"

    #renaming and ordering txt files
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
            mv --backup=numbered -i -f "$filename" "00000_AUTHOR_NOT_FOUND"/"$filename"
            continue
        fi

        author=$(echo "$firstlines" | sed -n 's/^.*[Aa][Uu][Tt][Hh][Oo][Rr]:[[:blank:]]*//p')
        if [ -z "$author" ]; then
            echo "Unable to find Author: in $filename; skipping" >&2
            mv --backup=numbered -i -f "$filename" "00000_AUTHOR_NOT_FOUND"/"$filename"
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

	#moving files not renamed
	mkdir -p "00001_NAMING_ERROR"
	for filename in *.txt; do
		mv --backup=numbered -i -f "$filename" "00001_NAMING_ERROR"/"$filename"
	done
    #end dangerous stuff
    
fi
