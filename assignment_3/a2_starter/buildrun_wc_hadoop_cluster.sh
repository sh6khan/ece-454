#!/bin/sh

export JAVA_HOME=/usr/java/jdk1.7.0_67-cloudera
export HADOOP_HOME=/usr
export CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`

echo --- Deleting
rm HadoopWC.jar
rm HadoopWC*.class

echo --- Compiling
$JAVA_HOME/bin/javac HadoopWC.java
if [ $? -ne 0 ]; then
    exit
fi

echo --- Jarring
$JAVA_HOME/bin/jar -cf HadoopWC.jar HadoopWC*.class

echo --- Running
INPUT=/tmp/english.1024MB
OUTPUT=/user/${USER}/a1_starter_code_output/

hdfs dfs -rm -R $OUTPUT
time $HADOOP_HOME/bin/hadoop jar HadoopWC.jar HadoopWC $INPUT $OUTPUT

hdfs dfs -ls $OUTPUT
