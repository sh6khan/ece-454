#!/bin/bash

unset JAVA_TOOL_OPTIONS
export JAVA_HOME=/usr/lib/jvm/java-1.8.0
export SCALA_HOME=/opt/scala-2.11.6
export SPARK_HOME=/opt/spark-2.1.0-bin-hadoop2.7
export CLASSPATH=.:"$SPARK_HOME/jars/*"

echo --- Deleting
rm Task4.jar
rm Task4*.class

echo --- Compiling
$SCALA_HOME/bin/scalac Task4.scala
if [ $? -ne 0 ]; then
    exit
fi

echo --- Jarring
$JAVA_HOME/bin/jar -cf Task4.jar Task4*.class

echo --- Running
INPUT=sample_input
OUTPUT=output_spark

rm -fr $OUTPUT
$SPARK_HOME/bin/spark-submit --master "local[*]" --class Task4 Task4.jar $INPUT $OUTPUT

cat $OUTPUT/*
