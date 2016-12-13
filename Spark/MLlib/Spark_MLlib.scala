// Databricks notebook source exported at Sat, 5 Nov 2016 15:23:00 UTC
//import classes
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils

// COMMAND ----------

//Load and parse the data file.
val sample_data = "/FileStore/tables/23phx7tu1475934034017/sample_libsvm_data.txt"
val data = MLUtils.loadLibSVMFile (sc ,sample_data)

// COMMAND ----------

// Split the data into training and test sets (30% held out for testing)
val splits = data.randomSplit(Array(0.7,0.3))
val (trainingData, testData) = (splits(0), splits(1))

// COMMAND ----------

//Train a DecisionTree model.
val numClasses = 2
val categoricalFeaturesInfo = Map[Int, Int]()
val impurity = "gini"
val maxDepth = 5
val maxBins = 32

val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins)

// COMMAND ----------

//Evaluate model on test instances and compute test error
val labelAndPreds = testData.map{point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}

val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
println("test Error = " + testErr)
println("Learned classification tree model:\n" + model.toDebugString)

// COMMAND ----------

// Train a RandomForest model.
//  Empty categoricalFeaturesInfo indicates all features are continuous.
import org.apache.spark.mllib.tree.RandomForest

val numClasses = 2
val categoricalFeaturesInfo = Map[Int, Int]()
val numTrees = 3 // Use more in practice.
val featureSubsetStrategy = "auto" // Let the algorithm choose.
val impurity = "gini"
val maxDepth = 4
val maxBins = 32

val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
  numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

// COMMAND ----------


// Evaluate model on test instances and compute test error
val labelAndPreds = testData.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}
val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
println("Test Error = " + testErr)
println("Learned classification forest model:\n" + model.toDebugString)

// COMMAND ----------

/*
MLlib: Old,RDD's.
ML: New, Pipelines, Dataframes.
*/
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.regression.DecisionTreeRegressor

val paramGrid = new ParamGridBuilder().build()
val evaluator = new MulticlassClassificationEvaluator()
  evaluator.setLabelCol("label")
  evaluator.setPredictionCol("prediction")
evaluator.setMetricName("precision")

//Cross validation Random Forest
val crossval_RF = new CrossValidator()
crossval_RF.setEstimator(new RandomForestRegressor)
crossval_RF.setEvaluator(evaluator) 
crossval_RF.setEstimatorParamMaps(paramGrid)
crossval_RF.setNumFolds(5)
val RF_CV = crossval_RF.fit(data.toDF())
val RF_metrics = RF_CV.avgMetrics

//Cross validation Descition tree
val crossval_DT = new CrossValidator()
crossval_DT.setEstimator(new DecisionTreeRegressor)
crossval_DT.setEvaluator(evaluator) 
crossval_DT.setEstimatorParamMaps(paramGrid)
crossval_DT.setNumFolds(5)
val DT_CV = crossval_DT.fit(data.toDF())
val DT_metrics = DT_CV.avgMetrics
