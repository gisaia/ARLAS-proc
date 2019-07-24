ThisBuild / version      := "0.3.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.11.8"
ThisBuild / organization := "io.arlas"

resolvers += "osgeo" at "http://download.osgeo.org/webdav/geotools/"
resolvers += "gisaia-ml" at s"https://dl.cloudsmith.io/${sys.env.getOrElse("CLOUDSMITH_PRIVATE_TOKEN", "basic")}/gisaia/private/maven"
resolvers += "boundless" at "http://repo.boundlessgeo.com/main"
resolvers += "jboss" at "https://repository.jboss.org/maven2/"

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

lazy val arlasProc = (project in file("."))
  .settings(
    name := "arlas-proc",
    libraryDependencies ++= spark,
    libraryDependencies ++= cassandra,
    libraryDependencies ++= elastic,
    libraryDependencies ++= geotools,
    libraryDependencies ++= arlas,
    libraryDependencies += scalaTest % Test

    )

//publish to external repo
ThisBuild / publishTo := { Some("Cloudsmith API" at "https://maven.cloudsmith.io/gisaia/private/") }
ThisBuild / pomIncludeRepository := { x => false }
ThisBuild / credentials += Credentials("Cloudsmith API", "maven.cloudsmith.io", sys.env.getOrElse("CLOUDSMITH_USER", ""), sys.env.getOrElse("CLOUDSMITH_API_KEY", ""))

//publish also assembly jar
test in assembly := {}
lazy val arlasProcAssembly = project
  .dependsOn(arlasProc)
  .settings(
      publishArtifact in (Compile, packageBin) := false,
      publishArtifact in (Compile, packageDoc) := false,
      publishArtifact in (Compile, packageSrc) := false,
      name := "arlas-proc-assembly",
      artifact in (Compile, assembly) ~= { art =>
          art.withClassifier(Some("assembly"))
      },
      addArtifact(artifact in (Compile, assembly), assembly)
      )
