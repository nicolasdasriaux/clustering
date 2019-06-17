# !/usr/bin/env bash

spark-submit \
--properties-file spark.properties \
--class clustering.LocationClusteringApp \
clustering-full.jar \
$1 \
$2 \
$3
