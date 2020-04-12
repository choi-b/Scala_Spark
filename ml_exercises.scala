//Programmer: Brian Choi
//Date: 4.9.2020
//ML Exercises

///////////////////////
// LINEAR REGRESSION //
///////////////////////

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.regression.LinearRegression

import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)

val spark = SparkSession.builder().getOrCreate()
val data = spark.read.option("header", "true").option("inferSchema", "true").option("multiline","true").format("csv").load("Ecommerce Customers.csv")

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors

val df = (data.select(data("Yearly Amount Spent").as("label"),
$"Avg Session Length", $"Time on App",
$"Time on Website", $"Length of Membership")) //use data.columns in spark shell to check

val assembler = (new VectorAssembler()
                  .setInputCols(Array("Avg Session Length", "Time on App",
                  "Time on Website", "Length of Membership")).setOutputCol("features"))

val output = assembler.transform(df).select($"label", $"features") //select them because vector assembler keeps the old columns

//Create a linear regression object
val lr = new LinearRegression()

//fit our model to our training data
val lrModel = lr.fit(output)

//print coefficients and intercept for lin reg
println(s"Coeff: ${lrModel.coefficients}, intercept: ${lrModel.intercept}")

//Summary results - show residuals, RMSE, MSE, and R^2
val trainingSummary = lrModel.summary
trainingSummary.residuals.show()
//print the other 3
println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
println(s"MSE: ${trainingSummary.meanSquaredError}")
println(s"R2: ${trainingSummary.r2}")

/////////////////////////
// LOGISTIC REGRESSION //
/////////////////////////

//data: fake ad Dataset
//predict: did the person click on it or not

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SparkSession

//to ignore a bunch of warnings
import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)

val spark = SparkSession.builder().getOrCreate()

//read in titanic data set
val data = spark.read.option("header","true").option("inferSchema","true").format("csv").load("advertising.csv")

//to show the columns we have
data.printSchema()
//daily time spent on site, age, area income, daily internet usage, etc...

//Grab the data that we want to use
//hit data.columns in the shell to easily copy column names

//take time stamp column, convert it into an hour first.
val timedata = data.withColumn("Hour",hour(data("Timestamp")))

val logregdata = (timedata.select(data("Clicked on Ad").as("label"),
                  $"Daily Time Spent on Site", $"Age", $"Area Income", $"Daily Internet Usage",
                  $"Hour", $"Male")
                  )

//Import vector assembler and vectors
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors

val assembler = (new VectorAssembler().
                  setInputCols(Array("Daily Time Spent on Site", "Age",
                  "Area Income", "Daily Internet Usage",
                  "Hour", "Male")).setOutputCol("features"))

//Use randomSplit to create a train test split 70/30
val Array(training, test) = logregdata.randomSplit(Array(0.7,0.3),seed=12345)

//Set up the pipeline
import org.apache.spark.ml.Pipeline
//Create a log reg object
val lr = new LogisticRegression()
//Create a new pipeline with the stages: assembler, lr
val pipeline = new Pipeline().setStages(Array(assembler, lr))
//Fit the pipeline to training set
val model = pipeline.fit(training)
//get the results on test set with transform
val results = model.transform(test)

//Model Evaluation
import org.apache.spark.mllib.evaluation.MulticlassMetrics

//Convert test results to an RDD
//use results.printSchema() to check the original & new columns (features, probability, prediction)
val predictionAndLabels = results.select($"prediction", $"label").as[(Double,Double)].rdd

val metrics = new MulticlassMetrics(predictionAndLabels)
//MulticlassMetrics is a bit broken,and will probably not be fixed since it's the old rdd method

//Print out confusion matrix
println("Confusion matrix:")
println(metrics.confusionMatrix)
