#!/bin/sh

unset JAVA_TOOL_OPTIONS
export JAVA_HOME=/usr/lib/jvm/java-1.8.0
export HADOOP_HOME=/opt/hadoop-2.7.2
export CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`

echo --- Deleting
rm Task1.jar
rm Task1*.class

echo --- Compiling
$JAVA_HOME/bin/javac Task1.java
if [ $? -ne 0 ]; then
    exit
fi

echo --- Jarring
$JAVA_HOME/bin/jar -cf Task1.jar Task1*.class

echo --- Running
INPUT=sample_input
OUTPUT=hadoop_task1_output

rm -fr $OUTPUT
$HADOOP_HOME/bin/hadoop jar Task1.jar Task1 $INPUT $OUTPUT

cat $OUTPUT/*
