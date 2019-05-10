#!/bin/bash

echo "Rimuovo cartelle di output: "
#hadoop fs -rm -r /user/st-darienzo/project/output/search
#hadoop fs -rm -r /user/st-darienzo/project/output/creation
echo "Lancio un job alla volta: "

echo "LANCIO."

#Number of reducers must be number of different authors
#input path; output path; num reducers
hadoop jar authAttribution.jar creation.AuthorAttributionCreation /user/st-darienzo/project/input/for_profiling/ /user/st-darienzo/project/output/creation/ 952

echo "LANCIO."

#Number of reducers must be number of unkown files
#input_files; output files; num reducers; path repository of profiles already created
hadoop jar authAttribution.jar search.AuthorAttributionSearch /user/st-darienzo/project/input/to_find/ /user/st-darienzo/project/output/search/ 18 /user/st-darienzo/project/output/creation/

