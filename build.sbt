ThisBuild / version      := "0.3.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.11.8"
ThisBuild / organization := "io.arlas"

val sparkSQL = "org.apache.spark" %% "spark-sql" % "2.3.1" % "provided"
val sparkMLlib = "org.apache.spark" %% "spark-mllib" % "2.3.1" % "provided"
val spark = Seq(sparkSQL,sparkMLlib)

val sparkCassandraConnector = "com.datastax.spark" %% "spark-cassandra-connector" % "2.3.2" % "provided"

val cassandra = Seq(sparkCassandraConnector)

val scalaTest = "org.scalatest" %% "scalatest" % "2.2.5"

lazy val arlasData = (project in file("."))
  .settings(
    name := "arlas-data",
    libraryDependencies ++= spark,
    libraryDependencies ++= cassandra,
    libraryDependencies += scalaTest % Test
  )

// publish artifact to GCP
enablePlugins(GcsPlugin)
gcsProjectId := sys.props.getOrElse("gcsProject", default = "arlas-lsfp")
gcsBucket := sys.props.getOrElse("gcsBucket", default = "arlas-data")+sys.props.getOrElse("gcsBucketPath", default = "/artifacts")