#!/bin/bash

# Usage: run-fuzz.sh <module>
# 
# Example: run-fuzz.sh specificreadercomplex

FUZZARCHIVE=${1}-fuzz.zip

if [[ ! -f "$FUZZARCHIVE" ]]; then
    echo "Could not find $FUZZARCHIVE, building"
    go-fuzz-build gopkg.in/avro.v0/fuzzes/$1
fi

go-fuzz -bin=./${FUZZARCHIVE} -workdir=./$1