#!/bin/bash

#This script creates a txt file containing
#the number of files in that directory and the number
#of files in each subdirectory.

rm count.txt

find . -type d -print0 | while read -d '' -r dir; do
    files=("$dir"/*)
    printf "%5d files in directory %s\n" "${#files[@]}" "$dir" >> count.txt
done

touch tmp.txt
sort count.txt > tmp.txt
cat tmp.txt > count.txt
rm tmp.txt
