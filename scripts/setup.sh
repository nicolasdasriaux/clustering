# !/usr/bin/env bash

rm -rf runenv
mkdir runenv
cd runenv
cp ../target/scala-2.11/clustering-full.jar .
cp ../scripts/clustering.sh .
cp ../scripts/spark.properties .
chmod u+x clustering.sh
