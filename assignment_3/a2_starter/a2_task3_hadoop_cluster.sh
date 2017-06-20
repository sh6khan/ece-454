#!/bin/sh

export JAVA_HOME=/usr/java/jdk1.7.0_67-cloudera
export HADOOP_HOME=/usr
export CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`

echo --- Deleting
rm Task3.jar
rm Task3*.class

echo --- Compiling
$JAVA_HOME/bin/javac Task3.java
if [ $? -ne 0 ]; then
    exit
fi

echo --- Jarring
$JAVA_HOME/bin/jar -cf Task3.jar Task3*.class

echo --- Running
INPUT=/tmp/in3.txt
OUTPUT=/user/${USER}/a1_starter_code_output/

hdfs dfs -rm -R $OUTPUT
time $HADOOP_HOME/bin/hadoop jar Task3.jar Task3 $INPUT $OUTPUT

hdfs dfs -ls $OUTPUT
