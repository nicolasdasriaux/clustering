Built on Spark 2.4.3

# Packaging App

* Start SBT
* Build uber-JAR

  ```
  assembly
  ```
* Get `clusterization-full.jar` in `target/scala-2.11`

# Testing with local Spark

```
spark-submit \
--class clustering.LocationClusteringApp \
target/scala-2.11/clusterization-full.jar \
src/test/resources/locations.json \
predictions \
clusters
```

# Launching in Spark over Yarn

* Copy `target/scala-2.11/clusterization-full.jar` to execution folder
* Copy `scripts/spark.properties` to execution folder
* Modify properties if necessary
* Copy `scripts/clustering.sh` file

* Make it executable
  ```
  chmod u+x clustering.sh
  ```

* Some environment variables might have to be set up depending on Spark Hadoop distribution and setup
* Launch job

  ```
  ./clustering.sh \
  <path-to-datafile> \
  <path-to-predictions-directory> \
  <path-to-clusters-directory>
  ```

# User Stories and Epics for Industrialization 

* **DONE** Explore sample dataset and identify data inconsistencies
  - **CLEANED** Coordinates can be inconsistently found in either (coordinates.latitude, coordinates.longitude) or (latitude, longitude).
  - Name contains id and slightly different address (STREET vs. St, ...)
  - **TO CLEAN** id: 7, 1 occurrence latitude: not relevant, 1 occurrence longitude: not relevant, implement merge
  - **TO CLEAN** id: 88, 2 occurrences, implement merge
  - **CLEAN** id: 89.0, should be 89
  - **CLEAN** id: 99.0, should be 99
  - id: 99.0, name: 98 - ..., non matching, should be 98?
  - id: 149, name: null
* **TODO** Migrate input file to JSON Line format for scalability
* **TODO** Discuss with data provider to decide to either improve source data or implement cleaning layer in app 
* **DONE** Implement cleaning layer in app
* **DONE** Implement clustering in app
* **DONE** Allow passing parameters / config for app
* **TODO** Implement merge by ID when data for an ID is dispatched over several rows

* **DONE** Package uber-JAR

* **TO DO** Run app with bigger files to evaluate data cleanig Spark and improve data cleaning accordingly

* **TO DO** Run app with big files to tune Spark config and observe actual program behaviour
  - Execution time
  - Memory use
  - Execution plan, shuffling, shuffle
  
* **TO DO** Optimize 'algorithm' to improve behaviour of app
* **TO DO** Publish predictions and clusters as Hive tables
* **TO DO** Implement daily scheduling or file upload triggered scheduling
* **TO DO** Override predictions and clusters every time the job is launched
* **TO DO** Package application with launcher and Spark config file

Epics

* **TO DO** Implement Continuous Integration
* **TO DO** Implement Continuous Deployment
