// scalafmt: { align.tokens.add = ["<-", "%%", "%", "="] }
import Dependencies._

val scala211 = "2.11.12"
val scala212 = "2.12.11"
val scala213 = "2.13.2"

ThisBuild / crossScalaVersions := Seq(scala213, scala212, scala211)

ThisBuild / scalaVersion := crossScalaVersions.value.head

lazy val commonSettings: Seq[Setting[_]] = Seq(
  bintrayRepository := {
    if((isSnapshot in ThisBuild).value) "snapshots" else "releases"
  },
  bintrayOrganization := Some("odsklm"),
  organization := "com.odsklm",
  licenses += ("MIT", url("http://opensource.org/licenses/MIT"))
)

lazy val root = (project in file("."))
  .settings(commonSettings)
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

lazy val types = (project in file("types"))
  .settings(commonSettings)
  .settings(
    name := "Salsa Beach types",
    moduleName := "salsa-beach-types",
    libraryDependencies ++= Seq(
      commonTestDeps
    ).flatten
  )
