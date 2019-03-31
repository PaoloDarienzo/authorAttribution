#!/bin/bash

#echo "Creo cartella su cloudera"
#mkdir bigDataProgetto

echo "Creo directory su HDFS: "
hadoop fs -mkdir /user/paolo/authorAttr/
hadoop fs -mkdir /user/paolo/authorAttr/input/short

echo "Sposto file di input da cloudera a HDFS: "
hadoop fs -put /bigDataProgetto/SubSetTest/short /user/paolo/authorAttr/input/
echo "Fatto."

echo "Rimuovo cartelle di output: "
hadoop fs -rm -r /user/paolo/authorAttr/output/*
echo "Fatto."

echo "Lancio un job alla volta: "
hadoop jar wordcounts.jar authorAttribution.WordCount /user/paolo/authorAttr/input/short /user/paolo/authorAttr/output/count/ 1
#hadoop jar wordcounts.jar authorAttribution.WordCount /user/paolo/authorAttr/input/short /user/paolo/authorAttr/output/count/ 1
#hadoop jar wordcounts.jar authorAttribution.WordCountAuth /user/paolo/authorAttr/input/short /user/paolo/authorAttr/output/countauth/ 1
#hadoop jar wordcounts.jar authorAttribution.WordCountInMap /user/paolo/authorAttr/input/short /user/paolo/authorAttr/output/cinmap/ 1
#hadoop jar wordcounts.jar authorAttribution.WordCountInMem /user/paolo/authorAttr/input/short /user/paolo/authorAttr/output/cinmem/ 1
#hadoop jar wordcounts.jar authorAttribution.WordCountInMapMulOut /user/paolo/authorAttr/input/short /user/paolo/authorAttr/output/cinmapouts/ 1
