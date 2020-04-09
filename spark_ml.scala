//Programmer: Brian Choi
//Date: 4.8.2020
//Notes on Spark Machine Learning

//1. Linear Regression
//data: US housing data
//predict: price of the house

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


//////////////////////////////////////////////////
