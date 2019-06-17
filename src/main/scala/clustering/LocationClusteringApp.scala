package clustering

import org.apache.spark.sql.SparkSession

object LocationClusteringApp {
  def main(args: Array[String]): Unit = {
    val locationJsonPath = args(0)
    val predictionsPath= args(1)
    val clustersPath = args(2)

    implicit val sparkSession: SparkSession = SparkSession.builder()
      .appName("location-clustering")
      .enableHiveSupport()
      .getOrCreate()

    LocationClusteringJob.run(locationJsonPath, predictionsPath, clustersPath)
  }
}
