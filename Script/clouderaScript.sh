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
hadoop fs -rm -r /user/paolo/authorAttr/output/authCompleteTest/veryshort/
#hadoop fs -rm -r /user/paolo/authorAttr/output/authCompleteTest/veryshort/result
#hadoop fs -rm -r /user/paolo/authorAttr/output/creation/veryshort
echo "Fatto."

echo "Lancio un job alla volta: "

echo "LANCIO."

#hadoop jar authAttribution.jar creation.AuthorAttributionCreation /user/paolo/authorAttr/input/SubSetTest/creation/veryshort /user/paolo/authorAttr/output/creation/veryshort/ 2

hadoop jar authAttribution.jar search.AuthorAttributionSearch /user/paolo/authorAttr/input/SubSetTest/search /user/paolo/authorAttr/output/authCompleteTest/veryshort/ 1

