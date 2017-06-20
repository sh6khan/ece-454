#!/bin/sh

unset JAVA_TOOL_OPTIONS
export JAVA_HOME=/usr/lib/jvm/java-1.8.0
export HADOOP_HOME=/opt/hadoop-2.7.2
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
INPUT=sample_input
OUTPUT=hadoop_task2_output

rm -fr $OUTPUT
$HADOOP_HOME/bin/hadoop jar Task2.jar Task2 $INPUT $OUTPUT

cat $OUTPUT/*
