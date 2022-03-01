

ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.11.8"
version := "1.0.1"
name := "Analyzer"
resolvers += "Default Repository" at "https://repo1.maven.org/maven2/"
resolvers += "Artima Maven Repository" at "https://repo.artima.com/releases"

libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"
libraryDependencies += "com.typesafe" % "config" % "1.2.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % "test"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.2"



assemblyJarName in assembly := "Analyzer.jar"
mainClass in assembly := Some("com.logAnalyzer.scala.AnalyzerSpark")