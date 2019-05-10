#!/bin/bash

#echo "Creo cartella su cloudera"
#mkdir bigDataProgetto

#echo "Creo directory su HDFS: "
#hadoop fs -mkdir /user/paolo/authorAttr/
#hadoop fs -mkdir /user/paolo/authorAttr/input/
#echo "Fatto."

#echo "Sposto file di input da cloudera a HDFS: "
#hadoop fs -put /bigDataProgetto/SubSetTest/ /user/paolo/authorAttr/input/
#echo "Fatto."

#echo "Rimuovo tutte le cartelle di output: "
#hadoop fs -rm -r /user/paolo/authorAttr/output/*
#echo "Fatto."

echo "Rimuovo cartelle di output: "

hadoop fs -rm -r /user/paolo/authorAttr/output/completeTest/search/
#hadoop fs -rm -r /user/paolo/authorAttr/output/completeTest/
#hadoop fs -rm -r /user/paolo/authorAttr/output/creation/veryshort
#hadoop fs -rm -r /user/paolo/authorAttr/output/creation/short
#hadoop fs -rm -r /user/paolo/authorAttr/output/creation/prova/
echo "Fatto."

echo "Lancio un job alla volta: "

echo "LANCIO."

#hadoop jar authAttribution.jar creation.AuthorAttributionCreation /user/paolo/authorAttr/input/completeTest/creation/ /user/paolo/authorAttr/output/creation/short/ 3

#hadoop jar authAttribution.jar creation.AuthorAttributionCreation /user/paolo/authorAttr/input/creation/veryshort /user/paolo/authorAttr/output/creation/veryshort/ 3

hadoop jar authAttribution.jar search.AuthorAttributionSearch /user/paolo/authorAttr/input/completeTest/search /user/paolo/authorAttr/output/completeTest/search/ 3 /user/paolo/authorAttr/output/creation/short


