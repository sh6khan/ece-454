#!/bin/bash

unset JAVA_TOOL_OPTIONS
export JAVA_HOME=/usr/lib/jvm/java-1.8.0
export SCALA_HOME=/opt/scala-2.11.6
export SPARK_HOME=/opt/spark-2.1.0-bin-hadoop2.7
export CLASSPATH=.:"$SPARK_HOME/jars/*"

echo --- Deleting
rm SparkWC.jar
rm SparkWC*.class

echo --- Compiling
$SCALA_HOME/bin/scalac SparkWC.scala
if [ $? -ne 0 ]; then
    exit
fi

echo --- Jarring
$JAVA_HOME/bin/jar -cf SparkWC.jar SparkWC*.class

echo --- Running
INPUT=sample_input
OUTPUT=output_spark

rm -fr $OUTPUT
$SPARK_HOME/bin/spark-submit --master "local[*]" --class SparkWC SparkWC.jar $INPUT $OUTPUT

cat $OUTPUT/*
