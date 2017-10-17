package com.pgssoft.movies

import com.typesafe.config.Config
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import spark.jobserver.{SparkJob, SparkJobValid, SparkJobValidation}

object RecommendationJob extends SparkJob {

  def validate(sc: SparkContext, config: Config): SparkJobValidation = SparkJobValid

  def runJob(sc: SparkContext, config: Config): Any = {

    val ss = SparkSession.builder().master("local[*]").appName("CYGU").getOrCreate()

    val sqlOpts = Map(
      "url" -> "jdbc:postgresql://10.10.34.177:5432/recommendation",
      "dbtable" -> "review",
      "user" -> "postgres",
      "password" -> "password"
    )

    val moviesOpts = Map(
      "url" -> "jdbc:postgresql://10.10.34.177:5432/recommendation",
      "dbtable" -> "movies",
      "user" -> "postgres",
      "password" -> "password"
    )

    val reviewDF = ss
      .read
      .format("jdbc")
      .options(options = sqlOpts)
      .load

    val moviesDF = ss
      .read
      .format("jdbc")
      .options(options = moviesOpts)
      .load

    val myRating = reviewDF.where("user_id=118205").select("user_id", "movie_id", "rate").rdd
    val movies = moviesDF.select("movie_id", "title").rdd.map(row => (row.getAs[Long](0).toInt, row.getString(1)))

    val rats = myRating.map(row => (row.getAs[Long](0).toInt, row.getAs[Long](1).toInt, row.getAs[Float](2).toDouble))

    val model = MatrixFactorizationModel.load(sc, "E:\\bestmodel")
    // read trained model from parquet file

    val candidates = movies.keys.filter(rats.id != _)

    println("Kandydaci %d".format(candidates.count()))

    val rdd = candidates.map((1, _))

    val recommendations = model
      .predict(rdd)
      .collect()
      .sortBy(-_.rating)
      .take(50)

    println("Liczba %d".format(recommendations.length))

    var i = 1
    println("Movies recommended for you:")
    recommendations.foreach { r =>
      println("%2d".format(i) + ": " + movies.id + " rating: " + r.rating)
      i += 1
    }
  }

  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    // set up environment

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("MovieLensALS")
      .set("spark.executor.memory", "2g")
    val sc = new SparkContext(conf)
  }

}
