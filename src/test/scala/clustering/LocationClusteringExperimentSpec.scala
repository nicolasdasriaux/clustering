package clustering

import java.nio.file.{Files, Paths}

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSpec, Matchers}

class LocationClusteringExperimentSpec extends FunSpec with Matchers with DataFrameSuiteBase {
  describe("LocationClusteringExperimentSpec") {
    ignore("should launch program") {
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

    it("should clean coordinates") {
      implicit val sparkSession: SparkSession = spark
      import sparkSession.implicits._

      val locations = Seq(
        Location(id = "1", coordinates = Coordinates(latitude = "1.1", longitude = "1.2"), latitude = null, longitude = null),
        Location(id = "2", coordinates = null, latitude = "2.1", longitude = "2.2")
      )

      val cleanedLocations = Seq(
        CleanedLocation(id = 1, latitude = 1.1, longitude = 1.2),
        CleanedLocation(id = 2, latitude = 2.1, longitude = 2.2)
      )

      LocationClusteringJob.cleanLocations(locations.toDF).as[CleanedLocation].collect().toList should ===(cleanedLocations)
    }
  }
}

case class Coordinates(latitude: String, longitude: String)
case class Location(id: String, coordinates: Coordinates, latitude: String, longitude: String)
case class CleanedLocation(id: Long, latitude: Double, longitude: Double)
