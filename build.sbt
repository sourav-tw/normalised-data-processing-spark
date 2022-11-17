ThisBuild / version := "0.1"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "normalized-data-processing"
  )

organization := "org.biki.spark"

autoScalaLibrary := false

val sparkVersion = "3.3.1"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

val postgresDependencies = Seq(
  "org.postgresql" % "postgresql" % "42.4.2"
)

val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.2.14" % Test
)

libraryDependencies ++= sparkDependencies ++ postgresDependencies ++ testDependencies