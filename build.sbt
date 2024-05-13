import Dependencies.*
import org.typelevel.scalacoptions.ScalacOptions

lazy val scala3                 = "3.4.1"
lazy val supportedScalaVersions = List(scala3)
lazy val scmUrl                 = "https://github.com/sky-uk/fs2-kafka-topic-loader"

ThisBuild / organization := "uk.sky"
ThisBuild / description  := "Read the contents of provided Kafka topics"
ThisBuild / licenses     := List("BSD New" -> url("https://opensource.org/licenses/BSD-3-Clause"))
ThisBuild / homepage     := Some(url(scmUrl))
ThisBuild / developers   := List(
  Developer(
    "Sky UK OSS",
    "Sky UK OSS",
    sys.env.getOrElse("SONATYPE_EMAIL", scmUrl),
    url(scmUrl)
  )
)

ThisBuild / semanticdbEnabled := true
ThisBuild / semanticdbVersion := scalafixSemanticdb.revision

lazy val root = (project in file("."))
  .settings(
    name               := "fs2-kafka-topic-loader",
    scalaVersion       := scala3,
    crossScalaVersions := supportedScalaVersions,
    CommonSettings.default,
    libraryDependencies ++= Seq(
      Cats.core,
      Cats.effect,
      Cats.log4cats,
      Cats.log4catsSlf4j,
      Fs2.core,
      Fs2.kafka,
      embeddedKafka,
      scalaTest,
      catsEffectTesting,
      logbackClassic
    )
  )

lazy val it = (project in file("it"))
  .settings(
    name         := "integration-test",
    scalaVersion := scala3,
    publish      := false
  )
  .dependsOn(root % "test->test;compile->compile")

Test / parallelExecution := false
Test / fork              := true

Global / onChangedBuildSource := ReloadOnSourceChanges
Global / scalafmtOnCompile    := true
