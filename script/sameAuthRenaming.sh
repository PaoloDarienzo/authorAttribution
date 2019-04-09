#!/bin/bash

nuovo_nome="Alexandre Dumas"

for filename in *.txt; do

	title=$(echo ${filename} | awk 'BEGIN{FS=",___," } {print $2}')
	
	new_name="$nuovo_nome,___,$title"
	mv --backup=numbered -i -f "$filename" "$new_name"

done

