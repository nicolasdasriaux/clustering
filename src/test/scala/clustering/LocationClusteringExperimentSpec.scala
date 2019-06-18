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

    it ("should work") {
      val sql = sqlContext
      import sql.implicits._

      sc.parallelize(1 to 10).sum() should ===(55)
    }
  }
}
