#!/bin/bash

source settings.sh

unset JAVA_TOOL_OPTIONS
export JAVA_HOME=/usr/lib/jvm/java-1.8.0
JAVA_CC=$JAVA_HOME/bin/javac
export CLASSPATH=".:gen-java:lib/*"

KV_PORT=`shuf -i 10000-10999 -n 1`
echo Port number: $KV_PORT

$JAVA_HOME/bin/java StorageNode `hostname` $KV_PORT $ZKSTRING /$USER
