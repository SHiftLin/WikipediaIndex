if [[ $1 == '' ]]; then
    echo 'need class name'
    exit
fi
target=./target/hadoop-1.0-SNAPSHOT.jar
timestamp=`date "+%s"`
zip -d  $target *LICENSE* *license*
set -e
hadoop jar $target  lsh.${1} sample.xml output${timestamp}
hadoop fs -get output${timestamp} /Users/lsh/Desktop/Workspace/hadoop/output${timestamp}
