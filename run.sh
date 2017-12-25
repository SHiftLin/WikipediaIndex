#!/bin/bash
if [[ $1 == '' ]]; then
    echo 'need class name'
    exit
fi

input=sample.xml
if [[ $1 == 'InvertedIndex' ]]; then
    if [[ $2 == '' ]]; then
        echo 'need input file'
        exit
    fi
    input=$2
fi

target=target/hadoop-1.0-SNAPSHOT.jar
timestamp=`date "+%s"`
zip -d  $target *LICENSE* *license*
set -e
hadoop jar $target  lsh.${1}  ${input} output${1}_${timestamp}
hadoop fs -get output${1}_${timestamp} /Users/lsh/Desktop/Workspace/hadoop/output${1}_${timestamp}
