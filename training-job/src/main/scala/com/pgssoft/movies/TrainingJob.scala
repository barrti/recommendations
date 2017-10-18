package com.pgssoft.movies

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import spark.jobserver.{SparkJob, SparkJobInvalid, SparkJobValid, SparkJobValidation}

import scala.util.Try

object TrainingJob extends SparkJob {


  override def runJob(sc: SparkContext, config: Config): Any = {

    val ss = SparkSession.builder().master("local[*]").appName("CYGU").getOrCreate()

    val sqlOpts = Map(
      "url" -> "jdbc:postgresql://10.10.34.177:5432/recommendation",
      "dbtable" -> "review",
      "user" -> "postgres",
      "password" -> "password"
    )

    val reviewDF = ss
      .read
      .format("jdbc")
      .options(options = sqlOpts)
      .load

    val rats = reviewDF.select("user_id", "movie_id", "rate").rdd
    val ratings = rats.map(row => Rating(row.getAs[Long](0).toInt, row.getAs[Long](1).toInt, row.getAs[Float](2).toDouble))
    val model = ALS.train(ratings, 12, 20, 0.1)

    model.save(sc, "E:\\bestmodel")
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    Try(config.getString("input.string"))
      .map(_ => SparkJobValid)
      .getOrElse(SparkJobInvalid("No input.string config param"))
  }
}
