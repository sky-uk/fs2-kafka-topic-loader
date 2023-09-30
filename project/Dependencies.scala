import sbt.*

object Dependencies {

  object Plugins {
    lazy val organizeImports = "com.github.liancheng" %% "organize-imports" % "0.6.0"
  }

  object Cats {
    lazy val core          = "org.typelevel" %% "cats-core"      % "2.9.0"
    lazy val effect        = "org.typelevel" %% "cats-effect"    % "3.4.11"
    lazy val log4cats      = "org.typelevel" %% "log4cats-core"  % "2.6.0"
    lazy val log4catsSlf4j = "org.typelevel" %% "log4cats-slf4j" % "2.6.0"
  }

  object Fs2 {
    lazy val core  = "co.fs2"          %% "fs2-core"  % "3.9.2"
    lazy val kafka = "com.github.fd4s" %% "fs2-kafka" % "3.0.1"
  }

  lazy val embeddedKafka     = "io.github.embeddedkafka" %% "embedded-kafka"                % "3.5.1"  % Test cross CrossVersion.for3Use2_13
  lazy val scalaTest         = "org.scalatest"           %% "scalatest"                     % "3.2.17" % Test
  lazy val catsEffectTesting = "org.typelevel"           %% "cats-effect-testing-scalatest" % "1.5.0"  % Test

  lazy val logbackClassic = "ch.qos.logback" % "logback-classic" % "1.4.11" % Runtime

}
