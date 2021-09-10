
name := "spark-optimization-2"

version := "0.1"

scalaVersion := "2.12.10"
val sparkVersion = "3.0.2"
val postgresVersion = "42.2.2"
val clickHouseJDBC = "0.3.1"
val postgres = "42.2.23"
val sparkClickhouseConnector = "0.23"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  // logging
  "org.apache.logging.log4j" % "log4j-api" % "2.4.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.4.1",
  // DBs
  "ru.yandex.clickhouse" % "clickhouse-jdbc" % clickHouseJDBC,
  "org.postgresql" % "postgresql" % postgres,
//  "io.clickhouse" % "spark-clickhouse-connector_2.11" % sparkClickhouseConnector,

)
