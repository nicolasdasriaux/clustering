package clustering

import java.nio.file.{Files, Paths}

import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSpec, Matchers}

class LocationClusteringExperimentSpec extends FunSpec with Matchers {
  describe("LocationClusteringExperimentSpec") {
    ignore("should work") {
      val locationJsonPath = Paths.get(getClass.getResource("/locations.json").getPath)
      val root = locationJsonPath.getParent
      val predictionsPath = root.resolve("predictions")
      val clustersPath = root.resolve("clusters")

      Files.delete(predictionsPath)

      implicit val sparkSession: SparkSession = SparkSession.builder()
        .appName("location-clustering-job")
        .master("local")
        .enableHiveSupport()
        .getOrCreate()

      LocationClusteringExperimentJob.run(locationJsonPath.toString, predictionsPath.toString, clustersPath.toString)
    }
  }
}
