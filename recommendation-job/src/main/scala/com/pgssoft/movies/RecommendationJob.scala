package com.pgssoft.movies

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.scalactic._
import spark.jobserver.api.{SparkJob => NewSparkJob, _}
import spark.jobserver.{NamedObjectPersister, NamedRDD, RDDPersister}

import scala.util.Try


object RecommendationJob extends NewSparkJob {

  override type JobData = Map[String, Long]
  override type JobOutput = Map[Int, Double]

  implicit def rddPersister[T]: NamedObjectPersister[NamedRDD[T]] = new RDDPersister[T]

  override def runJob(sc: SparkContext,
                      runtime: JobEnvironment,
                      data: JobData): JobOutput = {
    val model = new MatrixFactorizationModel(12,
      runtime.namedObjects.get[NamedRDD[(Int, Array[Double])]]("rdd:userFeatures").get.rdd,
      runtime.namedObjects.get[NamedRDD[(Int, Array[Double])]]("rdd:productFeatures").get.rdd
    )
    model.recommendProducts(data("userId").toInt, data("size").toInt)
      .map(x => (x.product, x.rating))
      .sortBy(x => x._2)
      .toMap
  }

  override def validate(sc: SparkContext,
                        runtime: JobEnvironment,
                        config: Config):
  JobData Or Every[ValidationProblem] = {
    Try(Good(
      Map("userId" -> config.getLong("userId"), "size" -> config.getLong("size")
      )))
      .getOrElse(Bad(One(SingleProblem("No userId"))))
  }
}
