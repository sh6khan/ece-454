#!/bin/bash

JAVA_HOME=/usr/lib/jvm/java-1.8.0/
SPARK_HOME=/opt/spark-2.1.0-bin-hadoop2.7
export CLASSPATH=.:"$SPARK_HOME/lib/*"

echo --- Deleting
rm *.jar
rm *.class

echo --- Compiling
$JAVA_HOME/bin/javac *.java
if [ $? -ne 0 ]; then
    exit
fi

echo --- Jarring
$JAVA_HOME/bin/jar -cf myCC.jar *.class

echo --- Running
#INPUT=input/huge.txt
#INPUT=input/large.txt
#INPUT=input/medium.txt
#INPUT=input/small.txt
INPUT=input/tiny.txt
OUTPUT=output
NUMCORES=4

# don't forget to use taskset!

rm -fr $OUTPUT
time $JAVA_HOME/bin/java -Xmx1g -cp myCC.jar CC $NUMCORES $INPUT $OUTPUT

echo --- Displaying partial output
cat $OUTPUT | head -n10
