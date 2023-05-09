import sbt.*

object Dependencies {
  object Plugins {
    lazy val organizeImports = "com.github.liancheng" %% "organize-imports" % "0.6.0"
  }

  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.2.15" % Test
}
