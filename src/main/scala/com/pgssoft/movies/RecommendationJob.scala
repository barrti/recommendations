//package com.pgssoft.movies
//
//import com.typesafe.config.Config
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.SQLContext
//import org.apache.spark.{SparkConf, SparkContext}
//import spark.jobserver.{SparkJobValid, SparkJobValidation, SparkSqlJob}
//
//object RecommendationJob extends SparkSqlJob {
//
//  def validate(sql: SQLContext, config: Config): SparkJobValidation = SparkJobValid
//
//  def runJob(sql: SQLContext, config: Config): Any = {
//
//    val myRating = sql.read.jdbc(null, null, null) // load myRating from database
//
//
//    val model = MatrixFactorizationModel.load(sql.sparkContext, "")
//    // read trained model from parquet file
//    val candidates = sql.read.jdbc(null, null, null) // loading candidates
//    val movies = sql.read.jdbc(null, null, null) //loading movies
//    val rdd: RDD[(Int, Int)] = candidates.map((1000, _))
//
//    val recommendations = model.predict(rdd)
//      .collect()
//      .sortBy(-_.rating)
//      .take(50)
//
//    var i = 1
//    println("Movies recommended for you:")
//    recommendations.foreach { r =>
//      println("%2d".format(i) + ": " + movies(r.product) + " rating: " + r.rating)
//      i += 1
//    }
//  }
//
//  def main(args: Array[String]) {
//
//    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
//    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
//
//    // set up environment
//
//    val conf = new SparkConf()
//      .setMaster("local[*]")
//      .setAppName("MovieLensALS")
//      .set("spark.executor.memory", "2g")
//    val sc = new SparkContext(conf)
//  }
//
//}
