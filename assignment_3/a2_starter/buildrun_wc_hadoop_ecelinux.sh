#!/bin/sh

unset JAVA_TOOL_OPTIONS
export JAVA_HOME=/usr/lib/jvm/java-1.8.0
export HADOOP_HOME=/opt/hadoop-2.7.2
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
INPUT=sample_input
OUTPUT=output_hadoop

rm -fr $OUTPUT
$HADOOP_HOME/bin/hadoop jar HadoopWC.jar HadoopWC $INPUT $OUTPUT

cat $OUTPUT/*
