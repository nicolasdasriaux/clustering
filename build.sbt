val root = (project in file(".")).settings(
  name := "clustering",
  version := "0.1",
  scalaVersion := "2.11.12",

  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.4.3" % Provided,
    "org.apache.spark" %% "spark-hive" % "2.4.3" % Provided,
    "org.apache.spark" %% "spark-mllib" % "2.4.3" % Provided,

    "org.scalatest" %% "scalatest" % "3.0.5" % Test,
    "com.holdenkarau" %% "spark-testing-base" % "2.4.3_0.12.0" % Test
  ),

  assemblyJarName in assembly := s"${name.value}-full.jar",
  assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),

  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", _*) => MergeStrategy.discard
    case "reference.conf" => MergeStrategy.concat
    case _ => MergeStrategy.first
  }
)
