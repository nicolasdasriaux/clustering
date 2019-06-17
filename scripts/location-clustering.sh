# !/usr/bin/env bash

spark-submit \
--property-files spark.properties \
--class clustering.LocationClusteringApp \
clusterization-full.jar \
$1 \
$2 \
$3
