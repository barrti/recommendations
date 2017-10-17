//package com.pgssoft.movies
//
//import java.io.File
//
//import com.pgssoft.movies.Application.computeRmse
//import com.typesafe.config.Config
//import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
//import org.apache.spark.sql.SQLContext
//import spark.jobserver.{SparkJobValid, SparkSqlJob}
//
//object TrainingJob extends SparkSqlJob{
//  override def runJob(sc: SQLContext,
//                      jobConfig: Config) = {
//    val ratings = sc.sparkContext.textFile(new File("C:\\dev\\movie-recomendation\\src\\main\\resources\\ratings.csv").toString).map { line =>
//      val fields = line.split(",")
//      // format: (timestamp % 10, Rating(userId, movieId, rating))
//      (fields(3).toLong % 10, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
//    }
//
//    val movies = sc.sparkContext.textFile(new File("C:\\dev\\movie-recomendation\\src\\main\\resources\\movies.csv").toString).map { line =>
//      val fields = line.split(",")
//      // format: (movieId, movieName)
//      (fields(0).toInt, fields(1))
//    }.collect().toMap
//
//    val numRatings = ratings.count()
//    val numUsers = ratings.map(_._2.user).distinct().count()
//    val numMovies = ratings.map(_._2.product).distinct().count()
//
//    println("Got " + numRatings + " ratings from "
//      + numUsers + " users on " + numMovies + " movies.")
//
//    // split ratings into train (60%), validation (20%), and test (20%) based on the
//    // last digit of the timestamp, add myRatings to train, and cache them
//
//    val numPartitions = 4
//    val training = ratings.filter(x => x._1 < 6)
//      .values
//      .repartition(numPartitions)
//      .cache()
//    val validation = ratings.filter(x => x._1 >= 6 && x._1 < 8)
//      .values
//      .repartition(numPartitions)
//      .cache()
//    val test = ratings.filter(x => x._1 >= 8).values.cache()
//
//    val numTraining = training.count()
//    val numValidation = validation.count()
//    val numTest = test.count()
//
//    println("Training: " + numTraining + ", validation: " + numValidation + ", test: " + numTest)
//
//    // train models and evaluate them on the validation set
//
//    val ranks = List(8, 12)
//    val lambdas = List(0.1, 10.0)
//    val numIters = List(10, 20)
//    var bestModel: Option[MatrixFactorizationModel] = None
//    var bestValidationRmse = Double.MaxValue
//    var bestRank = 0
//    var bestLambda = -1.0
//    var bestNumIter = -1
//    for (rank <- ranks; lambda <- lambdas; numIter <- numIters) {
//      val model = ALS.train(training, rank, numIter, lambda)
//      val validationRmse = computeRmse(model, validation, numValidation)
//      println("RMSE (validation) = " + validationRmse + " for the model trained with rank = "
//        + rank + ", lambda = " + lambda + ", and numIter = " + numIter + ".")
//      if (validationRmse < bestValidationRmse) {
//        bestModel = Some(model)
//        bestValidationRmse = validationRmse
//        bestRank = rank
//        bestLambda = lambda
//        bestNumIter = numIter
//      }
//    }
//
//    // evaluate the best model on the test set
//
//    val testRmse = computeRmse(bestModel.get, test, numTest)
//
//    println("The best model was trained with rank = " + bestRank + " and lambda = " + bestLambda
//      + ", and numIter = " + bestNumIter + ", and its RMSE on the test set is " + testRmse + ".")
//
//    // create a naive baseline and compare it with the best model
//
//    val meanRating = training.union(validation).map(_.rating).mean
//    val baselineRmse =
//      math.sqrt(test.map(x => (meanRating - x.rating) * (meanRating - x.rating)).mean)
//    val improvement = (baselineRmse - testRmse) / baselineRmse * 100
//    println("The best model improves the baseline by " + "%1.2f".format(improvement) + "%.")
//
//
//    bestModel take 1
//  }
//
//  override def validate(sc: SQLContext,
//                        config: Config) = SparkJobValid
//}