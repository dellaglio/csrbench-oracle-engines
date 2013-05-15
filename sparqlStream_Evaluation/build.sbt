name := "csrbench-engines"

organization := "eu.planetdata"

version := "0.0.1"

scalaVersion := "2.10.1"

crossPaths := false

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.0.11",
  "eu.planetdata" % "srbench" % "1.0.1",
  "junit" % "junit" % "4.7" % "test")

resolvers ++= Seq(
  DefaultMavenRepository,
  "Local ivy Repository" at "file://"+Path.userHome.absolutePath+"/.ivy2/local",
  "typesafe" at "http://repo.typesafe.com/typesafe/releases/",
  "aldebaran-releases" at "http://aldebaran.dia.fi.upm.es/artifactory/sstreams-releases-local"
 )

scalacOptions += "-deprecation"

EclipseKeys.skipParents in ThisBuild := false

EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource


