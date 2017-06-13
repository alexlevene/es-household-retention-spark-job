val proj_name = "SPARK_JOB_TEMPLATE"

name := proj_name

//version := Process("git rev-parse --short HEAD").lines.head
version := "0.1.0"
scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0" % "provided"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.8"
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.6.0"
libraryDependencies += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.8"
libraryDependencies += "junit" % "junit" % "4.10" % "test"

assemblyMergeStrategy in assembly <<= (assemblyMergeStrategy in assembly) { (old) =>
{
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x => old(x)
}
}

val proj_name_check = proj_name match {
  case "SPARK_JOB_TEMPLATE" =>
    println("ERROR: Project has name SPARK_JOB_TEMPLATE.")
    println("Please rename the project in build.sbt")
    System.exit(0)
  case s =>
}
