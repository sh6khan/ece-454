#!/bin/sh

export JAVA_HOME=/usr/java/jdk1.7.0_67-cloudera
export SCALA_HOME=/usr
export CDH_HOME=/opt/cloudera/parcels/CDH-5.11.0-1.cdh5.11.0.p0.34/

echo --- Deleting
rm Task2.jar
rm Task2*.class

echo --- Compiling
$SCALA_HOME/bin/scalac -J-Xmx1g Task2.scala
if [ $? -ne 0 ]; then
    exit
fi

echo --- Jarring
$JAVA_HOME/bin/jar -cf Task2.jar Task2*.class

echo --- Running
INPUT=/tmp/in0.txt
OUTPUT=/user/${USER}/a1_starter_code_output/

hdfs dfs -rm -R $OUTPUT
time $SPARK_HOME/bin/spark-submit --class Task2 Task2.jar $INPUT $OUTPUT

hdfs dfs -ls $OUTPUT