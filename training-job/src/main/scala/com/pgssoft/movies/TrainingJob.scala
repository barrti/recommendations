package com.pgssoft.movies

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.scalactic._
import spark.jobserver.api.{SparkJob => NewSparkJob, _}
import spark.jobserver.{NamedObjectPersister, NamedRDD, RDDPersister}

import scala.util.Try

object TrainingJob extends NewSparkJob {
  override type JobData = Boolean
  override type JobOutput = String

  implicit def rddPersister[T]: NamedObjectPersister[NamedRDD[T]] = new RDDPersister[T]

  override def runJob(sc: SparkContext,
                      runtime: JobEnvironment,
                      data: JobData): JobOutput = {
    val ss = SparkSession.builder().master("local[*]").appName("Recommendation Job").getOrCreate()
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
    val ratingRdd = reviewDF.select("user_id", "movie_id", "rate").rdd
    val ratings = ratingRdd.map(row => Rating(row.getAs[Long](0).toInt,
      row.getAs[Long](1).toInt,
      row.getAs[Float](2).toDouble))
    val model = ALS.train(ratings, 12, 20, 0.1)
    runtime.namedObjects.update("rdd:userFeatures",
      NamedRDD(model.userFeatures, forceComputation = false, StorageLevel.MEMORY_ONLY))
    runtime.namedObjects.update("rdd:productFeatures",
      NamedRDD(model.productFeatures, forceComputation = false, StorageLevel.MEMORY_ONLY))
    "Model has been trained"
  }

  override def validate(sc: SparkContext,
                        runtime: JobEnvironment,
                        config: Config):
  JobData Or Every[ValidationProblem] = {
    Try(Good(
      true
    )).getOrElse(Bad(One(SingleProblem("Error"))))
  }

}
