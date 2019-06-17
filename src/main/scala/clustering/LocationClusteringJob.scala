package clustering

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object LocationClusteringJob {
  def run(locationJsonPath: String, predictionsPath: String, clustersPath: String)(implicit sparkSession: SparkSession): Unit = {
    val rawLocationsDF = loadRawLocations(locationJsonPath)
    val locationsDF = cleanLocations(rawLocationsDF)
    val featuresDF = extractFeatures(locationsDF).cache()
    val locationClustering = performLocationClustering(featuresDF)
    saveLocationClustering(predictionsPath, clustersPath, locationClustering)
  }

  /**
    * TODO Promote JSON Line for scalability
    * TODO [DONE] Provide schema to avoid inference (double scan)
    */
  def loadRawLocations(locationJsonPath: String)(implicit sparkSession: SparkSession): DataFrame = {
    val locationSchema = StructType(Seq(
      StructField("id", StringType, true),
      StructField("name", StringType, true),
      StructField("address", StringType, true),
      StructField("latitude", StringType, true),
      StructField("longitude", StringType, true),

      StructField("coordinates",
        StructType(Seq(
          StructField("latitude", StringType, true),
          StructField("longitude", StringType, true)
        )),
        true
      ),

      StructField("position", StringType, true)
    ))

    val locationsDF = sparkSession.read
      .option("multiline", true) // TODO JSON Lines
      .option("mode", "FAILFAST")
      .schema(locationSchema)
      .json(locationJsonPath) // TODO [DONE] Make configurable

    locationsDF
  }

  /**
    * Fix various data inconsistencies
    * (coordinates.latitude, coordinates.longitude) vs. (latitude, longitude)
    * name contains id and slightly different address (STREET vs. St, ...)
    *
    * id: 7, 1 occurrence latitude: not relevant, 1 occurrence longitude: not relevant, implement merge
    * id: 88, 2 occurrences, implement merge
    * id: 89.0, should be 89
    * id: 99.0, should be 99
    * id: 99.0, name: 98 - ..., non matching, should be 98?
    * id: 149, name: null
    */
  def cleanLocations(rawLocationsDF: DataFrame)(implicit sparkSession: SparkSession): DataFrame = {
    import org.apache.spark.sql.functions._
    import sparkSession.implicits._

    val locationsDF = rawLocationsDF
      .withColumn("id", $"id".cast(LongType))
      .withColumn("latitude", coalesce($"latitude", $"coordinates.latitude").cast(DoubleType))
      .withColumn("longitude", coalesce($"longitude", $"coordinates.longitude").cast(DoubleType))
      .withColumn("_name_id", regexp_extract($"name", """(\d+) - (.*)""", 1).cast(LongType))
      .withColumn("_id_match", $"id" === $"_name_id")
      .withColumn("_name_address", regexp_extract($"name", """(\d+) - (.*)""", 2))
      .withColumn("_address_match", lower($"_name_address") === lower($"address"))
      .drop($"coordinates")
    locationsDF
  }

  def extractFeatures(locationsDF: DataFrame)(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    val coordinates = sparkSession.udf.register("coordinates", Functions.coordinates _)

    val featuresDF = locationsDF
      .where($"latitude".isNotNull && $"longitude".isNotNull)
      .select($"id", coordinates($"latitude", $"longitude").as("features"))

    featuresDF
  }

  def performLocationClustering(featuresDF: DataFrame): LocationClustering = {
    val kMeans = new KMeans().setK(3).setSeed(1l)
    val model = kMeans.fit(featuresDF)
    val predictionsDF = model.transform(featuresDF)

    val locationClusters = model.clusterCenters.toList
      .zipWithIndex
      .map { case (vector, index) => LocationCluster(index, vector(0), vector(1)) }

    LocationClustering(locationClusters, predictionsDF)
  }

  case class LocationClustering(locationClusters: Seq[LocationCluster], predictionsDF: DataFrame)
  case class LocationCluster(id: Int, latitude: Double, longitude: Double)

  object Functions {
    def coordinates(latitude: Double, longitude: Double): linalg.Vector = Vectors.dense(latitude, longitude)
  }

  def saveLocationClustering(predictionsPath: String, clustersPath: String, locationClustering: LocationClustering)(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    locationClustering.predictionsDF.write.parquet(predictionsPath)

    val locationClustersDF = sparkSession.sparkContext.parallelize(locationClustering.locationClusters).toDF
    locationClustersDF.write.parquet(clustersPath)
  }
}
