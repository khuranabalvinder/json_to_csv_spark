name := "axis_poc"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "com.github.agourlay" % "json-2-csv_2.12" % "0.4.2",
  "org.apache.spark" %% "spark-core" % "2.4.3",
  "org.apache.spark" %% "spark-sql" % "2.4.3",
  "com.holdenkarau" %% "spark-testing-base" % "2.4.3_0.12.0" % "test",
)