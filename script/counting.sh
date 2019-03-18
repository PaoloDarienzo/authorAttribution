#!/bin/bash

rm count.txt

find . -type d -print0 | while read -d '' -r dir; do
    files=("$dir"/*)
    printf "%5d files in directory %s\n" "${#files[@]}" "$dir" >> count.txt
done

touch tmp.txt
sort count.txt > tmp.txt
cat tmp.txt > count.txt
rm tmp.txt
