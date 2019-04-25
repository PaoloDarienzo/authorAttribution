#!/bin/bash

echo "Sposto files jar su cloudera: "
docker cp -a -L authAttribution.jar cloudera:/bigDataProgetto/
echo "Fatto."

#echo "Sposto file di input su cloudera: "
#docker cp -a -L ./SubSetTest cloudera:/bigDataProgetto/
#echo "Fatto."

echo "Aggiorno file script su cloudera: "
docker cp -a -L clouderaScript.sh cloudera:/bigDataProgetto/
echo "Fatto."

###########################################################

#From local shell to HDFS (through cloudera shell)
#docker exec -i cloudera bash < script.sh

#Open cloudera bash
#docker exec -it cloudera /bin/bash

#Deleting directories in HDFS (from cloudera shell)
#hadoop fs -rm -r /user/paolo/authorAttr/

#create directories in HDFS (from cloudera shell)
#hadoop fs -mkdir /user/paolo/authorAttr/
#hadoop fs -mkdir /user/paolo/authorAttr/input/

#From local filesytem to cloudera filesystem
#docker cp -a -L ./SubSetTest cloudera:/bigDataProgetto/

#From cloudera filesystem to HDFS (from cloudera shell)
#hadoop fs -put /SubSetTest /user/myname/authorAttr/input/

#Removing output directories from HDFS (from cloudera shell)
#hadoop fs -rm -r /user/paolo/authorAttr/output

#Esecuzione file java con 1 reducer (from cloudera shell)
#nome file del main:= pkg.class
#hadoop jar authattr.jar authorAttribution.AuthorAttribution /user/paolo/authorAttr/input/SubSetTest /user/paolo/authorAttr/output 1
