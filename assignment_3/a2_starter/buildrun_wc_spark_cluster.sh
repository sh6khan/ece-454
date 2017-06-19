#!/bin/sh

export JAVA_HOME=/usr/java/jdk1.7.0_67-cloudera
export SCALA_HOME=/usr
export CDH_HOME=/opt/cloudera/parcels/CDH-5.11.0-1.cdh5.11.0.p0.34/
export CLASSPATH=".:$CDH_HOME/jars/*"

echo --- Deleting
rm SparkWC.jar
rm SparkWC*.class

echo --- Compiling
$SCALA_HOME/bin/scalac -J-Xmx1g SparkWC.scala
if [ $? -ne 0 ]; then
    exit
fi

echo --- Jarring
$JAVA_HOME/bin/jar -cf SparkWC.jar SparkWC*.class

echo --- Running
INPUT=/tmp/english.1024MB
OUTPUT=/user/${USER}/a1_starter_code_output/

hdfs dfs -rm -R $OUTPUT
time $SPARK_HOME/bin/spark-submit --class SparkWC SparkWC.jar $INPUT $OUTPUT

hdfs dfs -ls $OUTPUT
