#!/bin/bash

#Launch this script inside a directory where there are a set of files
#to be renamed with the same author.
#Authors identified to have different alias:
#E. A. Poe, A. C. Doyle, Arthur Quiller-Couch, Lev Tolstoj, Alexandre Dumas (pere and fils), Charlotte Bronte,
#Thomas De Quincey, A. F. Mockler Ferryman, Gordon Stables, Charles Egbert Craddock.

new_author="Alexandre Dumas"

for filename in *.txt; do

	title=$(echo ${filename} | awk 'BEGIN{FS=",___," } {print $2}')
	
	new_name="$new_author,___,$title"
	mv --backup=numbered -i -f "$filename" "$new_name"

done

