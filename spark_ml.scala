//Programmer: Brian Choi
//Date Created: 4.8.2020
//Notes on Spark Machine Learning

////////////////////////////
/// 1. Linear Regression ///
/// data: US housing data //
/// predict: price of the house //

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.regression.LinearRegression

import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)

val spark = SparkSession.builder().getOrCreate()

val data = spark.read.option("header", "true").option("inferSchema", "true").option("multiline","true").format("csv").load("USA_Housing.csv")

//data.printSchema

// ("label", "features") -> use vector assembler
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors

val df = (data.select(data("Price").as("label"),
$"Avg Area Income", $"Avg Area House Age", $"Avg Area Number of Rooms",
$"Avg Area Number of Bedrooms", $"Area Population")) //use data.columns in spark shell to check

val assembler = (new VectorAssembler()
                  .setInputCols(Array("Avg Area Income", "Avg Area House Age",
                    "Avg Area Number of Rooms", "Avg Area Number of Bedrooms",
                    "Area Population")).setOutputCol("features"))

val output = assembler.transform(df).select($"label", $"features") //select them because vector assembler keeps the old columns

//label: price you're trying to predict
//features are now all in a single array


//Create a linear regression object
val lr = new LinearRegression()

//fit our model to our training data
val lrModel = lr.fit(output)

//Summary results
val trainingSummary = lrModel.summary
trainingSummary.residuals.show()

//in Spark-shell, type in trainingSummary. then hit tab to check out the available attributes
// ex) r2, predictions, rootMeanSquaredError, r2, etc..
trainingSummary.predictions
trainingSummary.r2
trainingSummary.rootMeanSquaredError


///////////////////////////////////////////////////
//2. Classification (Logistic Regression)/////////
/////////////////////////////////////////////////

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SparkSession

//<Example of using an 'object'>
//object lets you package a bunch of functions together
object LogisticRegressionWithElasticNetExample {

  def main(): Unit = {
    val spark = SparkSession
      .builder
      .appName("LogisticRegressionWithElasticNetExample")
      .getOrCreate()

    //load training data
    val training = spark.read.format("libsvm").load("sample_libsvm_data.txt")

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    //fit the model
    val lrModel = lr.fit(training)

    //print coeff and intercept for Logistic Reg
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    //stops the Spark session
    spark.stop()
  }
}

//Example
//data: Titanic Dataset
//predict: Predict who survived or not

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SparkSession

//to ignore a bunch of warnings
import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)

val spark = SparkSession.builder().getOrCreate()

//read in titanic data set
val data = spark.read.option("header","true").option("inferSchema","true").format("csv").load("titanic.csv")

//to show the columns we have
data.printSchema()


//Grab the data that we want to use
//hit data.columns in the shell to easily copy column names
val logregdataall = (data.select(data("Survived").as("label"),
$"Pclass", $"Name", $"Sex", $"Age", $"SibSp", $"Parch", $"Fare", $"Embarked"))

val logregdata = logregdataall.na.drop()

//use StringIndexer & OneHotEncoder
//Import multiple features to use
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer, VectorIndexer, OneHotEncoder}
import org.apache.spark.ml.linalg.Vectors

//Converting strings into numerical values
val genderIndexer = new StringIndexer().setInputCol("Sex").setOutputCol("SexIndex")
val embarkIndexer = new StringIndexer().setInputCol("Embarked").setOutputCol("EmbarkIndex")

//Convert numerical values into One Hot Encoding (0 or 1)
val genderEncoder = new OneHotEncoder().setInputCol("SexIndex").setOutputCol("SexVec")
val embarkEncoder = new OneHotEncoder().setInputCol("EmbarkIndex").setOutputCol("EmbarkVec")

//(label, features)

val assembler = (new VectorAssembler().
                  setInputCols(Array("Pclass","SexVec","Age","SibSp",
                  "Parch","Fare","EmbarkVec")).setOutputCol("features"))
//set these as an output (the features)

//Split train into train & test
val Array(training, test) = logregdata.randomSplit(Array(0.7, 0.3), seed = 12345) //70 % are training

//Set up the pipeline
import org.apache.spark.ml.Pipeline

val lr = new LogisticRegression()

// to make it easier to put in raw data and execute process all at once
val pipeline = new Pipeline().setStages(Array(genderIndexer, embarkIndexer,
                                          genderEncoder, embarkEncoder, assembler, lr))

val model = pipeline.fit(training)

//get results on test data
val results = model.transform(test)

import org.apache.spark.mllib.evaluation.MulticlassMetrics

//use results.printSchema() to check the original & new columns (features, probability, prediction)
val predictionAndLabels = results.select($"prediction", $"label").as[(Double,Double)].rdd

val metrics = new MulticlassMetrics(predictionAndLabels)
//MulticlassMetrics is a bit broken,and will probably not be fixed since it's the old rdd method

println("Confusion matrix:")
println(metrics.confusionMatrix)

//////////////////////////
// 3. Model Evaluation //
////////////////////////

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}

// <Documentation Example> //
val data = spark.read.format("libsvm").load("sample_linear_regression_data.txt")

// train test split
val Array(training, test) = data.randomSplit(Array(0.9,0.1),
                                             seed=12345) //90% training, 10% test

val lr = new LinearRegression()

val paramGrid = new ParamGridBuilder().addGrid(lr.regParam, Array(0.1,0.01)).addGrid(lr.fitIntercept).addGrid(lr.elasticNetParam, Array(0.0,0.5,1.0)).build()
//explore parameters with lr. + tab

//Run train validation split
val trainValidationSplit = new TrainValidationSplit().setEstimator(lr).setEvaluator(new RegressionEvaluator()).setEstimatorParamMaps(paramGrid).setTrainRatio(0.8)

val model = trainValidationSplit.fit(training)

model.transform(test).select("features", "label", "prediction").show()
//model.bestModel can return the best model.


// <Revisiting the Housing Price Example> //
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}

import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)

import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()

//Read data
val data = spark.read.option("header","true").option("inferSchema","true").format("csv").load("USA_Housing.csv")

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors

val df = data.select(data("Price").as("label"),$"Avg Area Income", $"Avg Area House Age",
$"Avg Area Number of Rooms", $"Avg Area Number of Bedrooms", $"Area Population")

val assembler = new VectorAssembler().setInputCols(Array("Avg Area Income", "Avg Area House", "Avg Area Number of Rooms", "Avg Area Number of Bedrooms", "Area Population")).setOutputCol("features")

//transform the df using the assembler
val output = assembler.transform(df).select($"label", $"features")


// Training and Test Data
val Array(training, test) = output.select("label", "features").randomSplit(Array(0.7,0.3),seed=12345)

//Model
val lr = new LinearRegression()

//ParamGrid - added a regularization parameter (just included two extreme values to compare the models)
val paramGrid = new ParamGridBuilder().addGrid(lr.regParam, Array(10000,0.1)).build()

//Train split (holdout)
val trainvalsplit = (new TrainValidationSplit()
                     .setEstimator(lr)
                     .setEvaluator(new RegressionEvaluator().setMetricName("r2")) //want the R^2 metric
                     .setEstimatorParamMaps(paramGrid)
                     .setTrainRatio(0.8))

val model = trainvalsplit.fit(training)

model.transform(test).select("features","label","prediction").show()
//model.validationMetrics
//current regularization params aren't too diff, play around with it.
