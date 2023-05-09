import DependencyVersions._

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.2.2"

lazy val root = (project in file("."))
  .settings(name := "parquet-merge")

libraryDependencies ++= Seq(
  "com.github.mjakubowski84" %% "parquet4s-fs2" % parquet4s,
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
  "co.fs2" %% "fs2-core" % fs2Version,
  "org.typelevel" %% "cats-effect" % catsEffectVersion,
  "co.fs2" %% "fs2-io" % fs2Version,
  "org.slf4j" % "slf4j-jdk14" % jdk14Logging,
  "org.typelevel" %% "cats-effect-testing-scalatest" % "1.5.0" % Test
)