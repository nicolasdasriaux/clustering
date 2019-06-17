package clustering

import org.apache.spark.sql.SparkSession

object LocationClusteringExperimentJob {
  import LocationClusteringJob._

  def run(locationJsonPath: String, predictionsPath: String, clustersPath: String)(implicit sparkSession: SparkSession): Unit = {
    import org.apache.spark.sql.functions._
    import sparkSession.implicits._

    val rawLocationsDF = loadRawLocations(locationJsonPath)
    rawLocationsDF.orderBy($"id").show(500) // DEBUG
    rawLocationsDF.printSchema() // DEBUG

    val locationsDF = cleanLocations(rawLocationsDF)
    locationsDF.orderBy($"id").show(500) // DEBUG
    locationsDF.printSchema() // DEBUG

    val duplicateLocationsDF = locationsDF
      .groupBy($"id")
      .agg(count($"id").as("count"))
      .where($"count" > 1)
      .join(locationsDF, "id")
      .drop($"count")

    duplicateLocationsDF.orderBy($"id").show(500) // DEBUG

    // TODO [DONE] Cache features for efficiency with iterative K-Mean
    val featuresDF = extractFeatures(locationsDF).cache()
    featuresDF.show() // DEBUG

    val locationClustering = performLocationClustering(featuresDF)
    locationClustering.predictionsDF.orderBy($"id").show(500) // DEBUG
    locationClustering.locationClusters.foreach(println) // DEBUG

    saveLocationClustering(predictionsPath, clustersPath, locationClustering)
  }
}
