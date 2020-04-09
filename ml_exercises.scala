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
