import sbt.*

object Dependencies {

  object Plugins {
    lazy val organizeImports = "com.github.liancheng" %% "organize-imports" % "0.6.0"
  }

  object Cats {
    lazy val core   = "org.typelevel" %% "cats-core"   % "2.9.0"
    lazy val effect = "org.typelevel" %% "cats-effect" % "3.4.10"
  }

  object Fs2 {
    lazy val core  = "co.fs2"          %% "fs2-core"  % "3.6.1"
    lazy val kafka = "com.github.fd4s" %% "fs2-kafka" % "3.0.1"
  }

  lazy val embeddedKafka = "io.github.embeddedkafka" %% "embedded-kafka" % "3.4.0"  % Test cross CrossVersion.for3Use2_13
  lazy val scalaTest     = "org.scalatest"           %% "scalatest"      % "3.2.15" % Test

  lazy val scalaLogging   = "com.typesafe.scala-logging" %% "scala-logging"   % "3.9.5"
  lazy val logbackClassic = "ch.qos.logback"              % "logback-classic" % "1.4.6" % Runtime

  val scala3Exclusions = Seq(
    "com.typesafe.scala-logging" % "scala-logging_2.13"
  )
}
