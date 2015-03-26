#!/usr/bin/env bash
index=$1
selector=$2
mkdir -p ./output/$index
node lib/index.js -i $index -a "$selector" > ./output/$index/$index.log