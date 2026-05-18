#!/bin/bash

set -e

export JAVA_HOME=/Users/harshsinha/Library/Java/JavaVirtualMachines/ms-11.0.30/Contents/Home

export PATH=$JAVA_HOME/bin:$PATH

mvn clean >/dev/null 2>&1

mvn install -DskipTests >/dev/null 2>&1

mvn -q -pl Main exec:java \
-Dexec.mainClass="com.example.Main"