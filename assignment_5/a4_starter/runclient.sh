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