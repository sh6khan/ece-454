#!/bin/bash

unset JAVA_TOOL_OPTIONS
export JAVA_HOME=/usr/lib/jvm/java-1.8.0
JAVA_CC=$JAVA_HOME/bin/javac
export CLASSPATH=".:gen-java:lib/*"


echo --- Running client

NUM_THREADS=8
NUM_SECONDS=8
KEY_SPACE=8

$JAVA_HOME/bin/java A4Client $NUM_THREADS $NUM_SECONDS $KEY_SPACE

echo --- Analyzing linearizability
$JAVA_HOME/bin/java ca.uwaterloo.watca.LinearizabilityTest execution.log scores.txt
echo Number of get operations returning junk: `cat scores.txt | grep 'Score = 2' | wc -l`
echo Number of other linearizability violations: `cat scores.txt | grep 'Score = 1' | wc -l`
