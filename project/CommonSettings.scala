import io.github.davidgregory084.TpolecatPlugin.autoImport.{
  tpolecatExcludeOptions,
  tpolecatScalacOptions,
  ScalacOptions
}
import sbt.*

object CommonSettings {

  val default: Seq[Def.Setting[?]] = Seq(
    tpolecatScalacOptions ++= Set(
      ScalacOptions.other("-no-indent"),
      ScalacOptions.other("-old-syntax"),
      ScalacOptions.other("-Wunused:all"),
      ScalacOptions.other("-Wnonunit-statement")
    ),
    Test / tpolecatExcludeOptions += ScalacOptions.warnNonUnitStatement
  )

}
