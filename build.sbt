ThisBuild / version      := (version in ThisBuild).value
ThisBuild / scalaVersion := "2.11.8"
ThisBuild / organization := "io.arlas"

resolvers += "osgeo" at "https://repo.osgeo.org/repository/release/"
resolvers += "gisaia" at "https://dl.cloudsmith.io/public/gisaia/public/maven/"
resolvers += "jboss" at "https://repository.jboss.org/maven2/"

val sparkVersion = "2.3.3"

val sparkSQL = "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
val sparkMLlib = "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided"
val spark = Seq(sparkSQL,sparkMLlib)

val scalaTest = "org.scalatest" %% "scalatest" % "2.2.5" % Test
val wiremockStandalone = "com.github.tomakehurst" % "wiremock-standalone" % "2.25.1" % Test
val tests = Seq(scalaTest, wiremockStandalone)

val elasticSearch = "org.elasticsearch" %% "elasticsearch-spark-20" % "7.4.2" % "provided"
val elastic = Seq(elasticSearch)

val gtReferencing = "org.geotools" % "gt-referencing" % "20.1" % "provided" exclude("javax.media", "jai_core")
val gtGeometry = "org.geotools" % "gt-geometry" % "20.1" % "provided" exclude("javax.media", "jai_core")
val geotools = Seq(gtReferencing, gtGeometry)

val arlasMl = "io.arlas" %% "arlas-ml" % "0.1.2"
val arlas = Seq(arlasMl)

lazy val arlasProc = (project in file("."))
  .settings(
    name := "arlas-proc",
    libraryDependencies ++= spark,
    libraryDependencies ++= elastic,
    libraryDependencies ++= geotools,
    libraryDependencies ++= arlas,
    libraryDependencies ++= tests

    )

//ensures a single instance of wiremock at a time, during tests
Test / parallelExecution := false

//publish to external repo
ThisBuild / publishTo := { Some("Cloudsmith API" at "https://dl.cloudsmith.io/public/gisaia/public/maven/") }
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

//sbt-release
import ReleaseTransformations._
import ReleasePlugin.autoImport._
import sbtrelease.{Git, Utilities}
import Utilities._
val deployBranch = "master"
def merge: (State) => State = { st: State =>
  val git = st.extract.get(releaseVcs).get.asInstanceOf[Git]
  //TODO manage git redirecting with no reason to stderr
  git.cmd("status") ! st.log
  val curBranch = (git.cmd("rev-parse", "--abbrev-ref", "HEAD") !!).trim
  st.log.info(s"####### current branch: $curBranch")
  git.cmd("checkout", deployBranch) ! st.log
  st.log.info(s"####### pull $deployBranch")
  git.cmd("pull") ! st.log
  st.log.info(s"####### merge")
  git.cmd("merge", curBranch, "--no-ff", "--no-edit") ! st.log
  st.log.info(s"####### push")
  git.cmd("push", "origin", s"$deployBranch:$deployBranch") ! st.log
  st.log.info(s"####### checkout $curBranch")
  git.cmd("checkout", curBranch) ! st.log
  st.log.info(s"####### pull $curBranch")
  git.cmd("pull", "origin", curBranch) ! st.log
  st.log.info(s"####### rebase origin/$deployBranch")
  git.cmd("rebase", s"origin/${deployBranch}") ! st.log
  st
}

lazy val mergeReleaseVersionAction = { st: State =>
  val newState = merge(st)
  newState
}

val mergeReleaseVersion = ReleaseStep(mergeReleaseVersionAction)

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  pushChanges,                //to make sure develop branch is pulled
  tagRelease,
  mergeReleaseVersion,        //will merge into master and push
  setNextVersion,
  commitNextVersion,
  pushChanges
  )