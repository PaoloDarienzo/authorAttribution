#!/bin/bash

#This script reformat the set of function words;
#Said set was copy-pasted from a pubblication.

touch tmp.txt
touch tmp2.txt

sed 's/,//g' func_words.txt > tmp.txt

for word in $(<tmp.txt)
	do
		echo "\"$word\"," >> tmp2.txt
	done
	
rm tmp.txt
