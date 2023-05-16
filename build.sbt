import Dependencies.*

lazy val scala3                 = "3.2.2"
lazy val scala213               = "2.13.10"
lazy val supportedScalaVersions = List(scala3, scala213)
lazy val scmUrl                 = "https://github.com/sky-uk/fs2-kafka-topic-loader"

ThisBuild / organization := "uk.sky"
ThisBuild / description  := "Read the contents of provided Kafka topics"
ThisBuild / licenses     := List("BSD New" -> url("https://opensource.org/licenses/BSD-3-Clause"))

ThisBuild / semanticdbEnabled := true
ThisBuild / semanticdbVersion := scalafixSemanticdb.revision

ThisBuild / scalafixDependencies += Dependencies.Plugins.organizeImports

tpolecatScalacOptions ++= Set(ScalacOptions.source3)

lazy val root = (project in file("."))
  .settings(
    name               := "fs2-kafka-topic-loader",
    scalaVersion       := scala213, // TODO - for development to get unused warnings
    crossScalaVersions := supportedScalaVersions,
    libraryDependencies ++= Seq(
      Cats.core,
      Cats.effect,
      Fs2.core,
      Fs2.kafka,
      embeddedKafka,
      scalaTest,
      scalaLogging,
      logbackClassic
    )
  )

lazy val it = (project in file("it"))
  .settings(
    name         := "integration-test",
    scalaVersion := scala213, // TODO - for development to get unused warnings
    publish      := false,
    libraryDependencies ++= Seq(
      Cats.core,
      Cats.effect,
      Fs2.core,
      Fs2.kafka,
      embeddedKafka,
      scalaTest,
      scalaLogging,
      logbackClassic
    )
  )
  .dependsOn(root % "test->test;compile->compile")

/** Scala 3 doesn't support two rules yet - RemoveUnused and ProcedureSyntax. So we require a different scalafix config
  * for Scala 3
  *
  * RemoveUnused relies on -warn-unused which isn't available in scala 3 yet -
  * https://scalacenter.github.io/scalafix/docs/rules/RemoveUnused.html
  *
  * ProcedureSyntax doesn't exist in Scala 3 - https://scalacenter.github.io/scalafix/docs/rules/ProcedureSyntax.html
  */
ThisBuild / scalafixConfig := {
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((3, _)) => Some((ThisBuild / baseDirectory).value / ".scalafix3.conf")
    case _            => None
  }
}

ThisBuild / excludeDependencies ++= {
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((3, _)) => Dependencies.scala3Exclusions
    case _            => Seq.empty
  }
}

Test / parallelExecution := false
Test / fork              := true

Global / onChangedBuildSource := ReloadOnSourceChanges
Global / scalafmtOnCompile    := true
