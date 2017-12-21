#!/bin/bash
# if [[ $1 == '' ]]; then
#     echo 'need class name'
#     exit
# fi
target=./target/hadoop-1.0-SNAPSHOT.jar
scp $target hadoop:~/
#ssh hadoop "./run.sh ${1}"
