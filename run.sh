#!/bin/bash

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <target> <command>"
  echo "target: cleaning/crash, cleaning/weather, profiling/crash, profiling/weather"
  echo "command: run, clean"
  exit 1
fi

TARGET=$1
COMMAND=$2

cd $TARGET
make $COMMAND
