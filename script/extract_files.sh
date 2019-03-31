#!/bin/bash

for dir in *; do
		#if the file is a directory
		if [ -d "$dir" ]; then
		  #move up by one the file(s)
		  mv "$dir"/*.txt .
		  #deleting directory (with content non-txt)
		  rm -r "$dir"
		fi
	done
