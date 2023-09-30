import org.typelevel.scalacoptions.ScalacOptions

tpolecatScalacOptions ++= Set(ScalacOptions.source3)

addCompilerPlugin("org.typelevel" % "kind-projector" % "0.13.2" cross CrossVersion.full)
