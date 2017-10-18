package com.pgssoft.movies

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import spark.jobserver.{SparkJob, SparkJobValid, SparkJobValidation}

object RecommendationJob extends SparkJob {

  def validate(sc: SparkContext, config: Config): SparkJobValidation = SparkJobValid

  def runJob(sc: SparkContext, config: Config): Any = {

    val model = MatrixFactorizationModel.load(sc, "E:\\bestmodel")
    val rc = model.recommendProducts(config.getInt("user_id"), config.getInt("size"))
    rc.map(x => (x.product, x.rating)).sortBy(x => x._2).toMap

  }

}