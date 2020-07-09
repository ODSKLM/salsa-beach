// scalafmt: { align.tokens.add = ["<-", "%%", "%", "="] }
import Dependencies._

val scala211 = "2.11.12"
val scala212 = "2.12.11"
val scala213 = "2.13.2"

crossScalaVersions in ThisBuild := Seq(scala213, scala212, scala211)

scalaVersion in ThisBuild := crossScalaVersions.value.head

lazy val root = (project in file("."))
  .settings(
    name := "Salsa Beach",
    moduleName := "salsa-beach",
    libraryDependencies ++= Seq(
      commonTestDeps,
      dependencies,
      hBaseTestDeps
    ).flatten
  )
  .aggregate(types)
  .dependsOn(types)
  .settings(skip in publish := true)

lazy val types = (project in file("types"))
  .settings(
    name := "Salsa Beach types",
    moduleName := "salsa-beach-types",
    libraryDependencies ++= Seq(
      commonTestDeps
    ).flatten
  )
