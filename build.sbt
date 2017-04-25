
organization := "com.github.brainlag"

name := "JavaNSQClient"

version := "1.0-MI"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "org.apache.logging.log4j" % "log4j-api" % "2.2",
  "org.apache.logging.log4j" % "log4j-core" % "2.2",
  "io.netty" % "netty-all" % "4.0.28.Final",
  "io.netty" % "netty-tcnative" % "1.1.33.Fork2" % "test",
  "org.apache.commons" % "commons-pool2" % "2.3",
  "junit" % "junit" % "4.12" % "test",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.5.1",
  "com.google.guava" % "guava" % "18.0"
)
