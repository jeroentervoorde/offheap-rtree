name := """rtree"""

version := "1.0"

scalaVersion := "2.11.7"

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)

// Change this to another test framework if you prefer
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

// Uncomment to use Akka
libraryDependencies += "com.meetup" %% "archery" % "0.5.0-SNAPSHOT"

libraryDependencies += "sh.den" %% "scala-offheap" % "0.1"

libraryDependencies += "com.vividsolutions" % "jts" % "1.13"

