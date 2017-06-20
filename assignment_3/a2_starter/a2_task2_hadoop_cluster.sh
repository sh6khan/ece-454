#!/bin/sh

export JAVA_HOME=/usr/java/jdk1.7.0_67-cloudera
export HADOOP_HOME=/usr
export CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`

echo --- Deleting
rm Task2.jar
rm Task2*.class

echo --- Compiling
$JAVA_HOME/bin/javac Task2.java
if [ $? -ne 0 ]; then
    exit
fi

echo --- Jarring
$JAVA_HOME/bin/jar -cf Task2.jar Task2*.class

echo --- Running
INPUT=/tmp/sample_input/
OUTPUT=/user/${USER}/a1_starter_code_output/

hdfs dfs -rm -R $OUTPUT
time $HADOOP_HOME/bin/hadoop jar Task2.jar Task2 $INPUT $OUTPUT

hdfs dfs -ls $OUTPUT
