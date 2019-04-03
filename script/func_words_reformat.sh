#!/bin/bash

touch tmp.txt
touch tmp2.txt

sed 's/,//g' func_words.txt > tmp.txt

for word in $(<tmp.txt)
	do
		echo "\"$word\"," >> tmp2.txt
	done
	
rm tmp.txt
