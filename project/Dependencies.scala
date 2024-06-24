import sbt.*

object Dependencies {

  object Cats {
    lazy val core          = "org.typelevel" %% "cats-core"      % "2.12.0"
    lazy val effect        = "org.typelevel" %% "cats-effect"    % "3.5.4"
    lazy val log4cats      = "org.typelevel" %% "log4cats-core"  % "2.7.0"
    lazy val log4catsSlf4j = "org.typelevel" %% "log4cats-slf4j" % "2.7.0"
  }

  object Fs2 {
    lazy val core  = "co.fs2"          %% "fs2-core"  % "3.10.2"
    lazy val kafka = "com.github.fd4s" %% "fs2-kafka" % "3.5.1"
  }

  lazy val embeddedKafka     = "io.github.embeddedkafka" %% "embedded-kafka"                % "3.6.1"  % Test
  lazy val scalaTest         = "org.scalatest"           %% "scalatest"                     % "3.2.18" % Test
  lazy val catsEffectTesting = "org.typelevel"           %% "cats-effect-testing-scalatest" % "1.5.0"  % Test

  lazy val logbackClassic = "ch.qos.logback" % "logback-classic" % "1.5.6" % Runtime
}
