#!/bin/bash

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <target> <command>"
  echo "target: cleaning, profiling/crash, profiling/weather"
  echo "command: run, clean"
  exit 1
fi

TARGET=$1
COMMAND=$2

cd $TARGET
# Redirect to Makefile in subfolders
# The Java and Hadoop command lines are in those Makefiles
make $COMMAND
