ThisBuild / version      := "0.3.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.11.8"
ThisBuild / organization := "io.arlas"

//https://github.com/sbt/sbt/issues/1138
fullResolvers := Seq(
                 "boundless" at "http://repo.boundlessgeo.com/main",
                 "osgeo" at "http://download.osgeo.org/webdav/geotools/",
                 "jboss" at "https://repository.jboss.org/maven2/",
                 "gisaia-ml" at s"https://dl.cloudsmith.io/${sys.env.getOrElse("CLOUDSMITH_TOKEN", "basic")}/gisaia/ml/maven",
                 "geotoolkit" at "http://maven.geotoolkit.org/",
                 DefaultMavenRepository,)

val sparkSQL = "org.apache.spark" %% "spark-sql" % "2.3.1" % "provided"
val sparkMLlib = "org.apache.spark" %% "spark-mllib" % "2.3.1" % "provided"
val spark = Seq(sparkSQL,sparkMLlib)

val sparkCassandraConnector = "com.datastax.spark" %% "spark-cassandra-connector" % "2.3.2" % "provided"

val cassandra = Seq(sparkCassandraConnector)

val scalaTest = "org.scalatest" %% "scalatest" % "2.2.5"

val elasticSearch = "org.elasticsearch" %% "elasticsearch-spark-20" % "6.4.0" % "provided"
val elastic = Seq(elasticSearch)

val gtReferencing = "org.geotools" % "gt-referencing" % "20.1" % "provided"
val gtGeometry = "org.geotools" % "gt-geometry" % "20.1" % "provided"
val geotools = Seq(gtReferencing, gtGeometry)

val arlasMl = "io.arlas" %% "arlas-ml" % "0.1.0"
val arlas = Seq(arlasMl)

lazy val arlasData = (project in file("."))
  .settings(
    name := "arlas-proc",
    libraryDependencies ++= geotools,
    libraryDependencies ++= spark,
    libraryDependencies ++= cassandra,
    libraryDependencies ++= elastic,
    libraryDependencies ++= arlas,
    libraryDependencies += scalaTest % Test
  )

// publish artifact to GCP
enablePlugins(GcsPlugin)
gcsProjectId := sys.props.getOrElse("gcsProject", default = "arlas-lsfp")
gcsBucket := sys.props.getOrElse("gcsBucket", default = "arlas-proc")+sys.props.getOrElse("gcsBucketPath", default = "/artifacts")

gcsLocalArtifactPath := (assemblyOutputPath in assembly).value
publish := publish.dependsOn(assembly).value
test in assembly := {}
assemblyJarName in assembly := s"${name.value}_${version.value}-assembly.jar"