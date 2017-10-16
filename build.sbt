name := "movie-recomendation"

version := "1.0"

scalaVersion := "2.11.9"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.2.0"

resolvers += "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven"

libraryDependencies += "spark.jobserver" %% "job-server-api" % "0.7.0" % "provided"

libraryDependencies += "spark.jobserver" %% "job-server-extras" % "0.7.0" % "provided"