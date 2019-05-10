#!/bin/bash

#Launch this script inside a directory where there are a set of files
#to be renamed with the same author.
#Authors identified to have different alias:
#E. A. Poe, A. C. Doyle, Fedor Dostoevskij, Arthur Quiller-Couch, Lev Tolstoj, Alexandre Dumas (pere and fils), Charlotte Bronte,
#Thomas De Quincey, Gordon Stables, Charles Egbert Craddock.

new_author="Edgar-Allan-Poe"
#new_author="Arthur-Conan-Doyle"
#new_author="Fedor-Dostoevskij"
#new_author="Arthur-Quiller-Couch"
#new_author="Lev-Tolstoj"
#new_author="Alexandre-Dumas"
#new_author="Alexandre-Dumas-(pere)"
#new_author="Alexandre-Dumas-(fils)"
#new_author="Thomas-De-Quincey"
#new_author="Gordon-Stables"
#new_author="Charles-Egbert-Craddock"

for filename in *.txt; do

	title=$(echo ${filename} | awk 'BEGIN{FS=",___," } {print $2}')
	
	new_name="$new_author,___,$title"
	mv --backup=numbered -i -f "$filename" "$new_name"

done

