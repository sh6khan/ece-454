#!/bin/sh

export JAVA_HOME=/usr/java/jdk1.7.0_67-cloudera
export SCALA_HOME=/usr
export CDH_HOME=/opt/cloudera/parcels/CDH-5.11.0-1.cdh5.11.0.p0.34/
export CLASSPATH=".:$CDH_HOME/jars/*"

echo --- Deleting
rm Task3.jar
rm Task3*.class

echo --- Compiling
$SCALA_HOME/bin/scalac -J-Xmx1g Task3.scala
if [ $? -ne 0 ]; then
    exit
fi

echo --- Jarring
$JAVA_HOME/bin/jar -cf Task3.jar Task3*.class

echo --- Running
INPUT=/tmp/sample_input
OUTPUT=/user/${USER}/a1_starter_code_output/

hdfs dfs -rm -R $OUTPUT
time $SPARK_HOME/bin/spark-submit --class Task3 Task3.jar $INPUT $OUTPUT

hdfs dfs -ls $OUTPUT
