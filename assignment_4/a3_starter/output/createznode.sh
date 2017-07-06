#!/bin/bash

source settings.sh

unset JAVA_TOOL_OPTIONS
export JAVA_HOME=/usr/lib/jvm/java-1.8.0
JAVA_CC=$JAVA_HOME/bin/javac
export CLASSPATH=".:lib/*"


echo --- Creating ZooKeeper node
./build.sh
$JAVA_HOME/bin/java CreateZNode $ZKSTRING /$USER
