#!/bin/bash

#This script extracts each file from 
#all the directories it found and then remove
#said directories.

for dir in *; do
		#if the file is a directory
		if [ -d "$dir" ]; then
		  #move up by one the file(s)
		  mv "$dir"/*.txt .
		  #deleting directory (with content non-txt)
		  rm -r "$dir"
		fi
	done
