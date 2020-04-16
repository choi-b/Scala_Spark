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


////////////////////////
// K Means Clustering //
////////////////////////

//Data: wholesale customers data (UCI repository)
//task: cluster data

import org.apache.spark.sql.SparkSession
import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)

val spark = SparkSession.builder().getOrCreate()

//Import clustering algorithm
import org.apache.spark.ml.clustering.KMeans

val dataset = spark.read.option("header","true").option("inferSchema","true").csv("Wholesale customers data.csv")

//subset feature_data
val feature_data = dataset.select($"Fresh",$"Milk",$"Grocery",$"Frozen",$"Detergents_Paper",$"Delicassen")

//import vectorasembler and vectors
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors

//Create a vector assembler
val assembler = new VectorAssembler().setInputCols(Array("Fresh","Milk","Grocery","Frozen","Detergents_Paper","Delicassen")).setOutputCol("features")

//Training data
val training_data = assembler.transform(feature_data).select("features")

//Create a K means model with K=3
val kmeans = new KMeans().setK(3)

//Fit k-means model to training data.
val model = kmeans.fit(training_data)

//Evaluate clustering by computing within set sum of squared errors (WSSSE)
val WSSSE = model.computeCost(training_data)
println(s"The Within Set Sum of Squared Errors = ${WSSSE}")

//////////////////////////////////
// Principal Component Analysis //
//////////////////////////////////

//Data: cancer data
//Task: reduce 30 features to 4 principal components

// Import Spark  and Create a Spark Session
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().appName("PCA_Example").getOrCreate()

// Use Spark to read in the Cancer_Data file.
val data = spark.read.option("header","true").option("inferSchema","true").format("csv").load("Cancer_Data")

// Print the Schema of the data
data.printSchema()

// Import PCA, VectorAssembler and StandardScaler from ml.feature
import org.apache.spark.ml.feature.{PCA,StandardScaler,VectorAssembler}

// Import Vectors from ml.linalg
import org.apache.spark.ml.linalg.Vectors

// Use VectorAssembler to convert the input columns as a single "features" column

val colnames = (Array("mean radius", "mean texture", "mean perimeter", "mean area", "mean smoothness",
"mean compactness", "mean concavity", "mean concave points", "mean symmetry", "mean fractal dimension",
"radius error", "texture error", "perimeter error", "area error", "smoothness error", "compactness error",
"concavity error", "concave points error", "symmetry error", "fractal dimension error", "worst radius",
"worst texture", "worst perimeter", "worst area", "worst smoothness", "worst compactness", "worst concavity",
"worst concave points", "worst symmetry", "worst fractal dimension"))

val assembler = new VectorAssembler().setInputCols(colnames).setOutputCol("features")

// Transform df to a single column: features
val output = assembler.transform(data).select($"features")

//Normalization is usually preferred but not required

// Use StandardScaler to normalize it (documentation example)
val scaler = (new StandardScaler()
  .setInputCol("features")
  .setOutputCol("scaledFeatures")
  .setWithStd(true)
  .setWithMean(false))

// Compute summary statistics by fitting the StandardScaler.
val scalerModel = scaler.fit(output)

// Use transform() off of this scalerModel object to create scaled data
val scaledData = scalerModel.transform(output)

// Create a PCA() object to apply PCA
val pca = (new PCA()
  .setInputCol("scaledFeatures")
  .setOutputCol("pcaFeatures")
  .setK(4)
  .fit(scaledData))

// Transform the scaled data
val pcaDF = pca.transform(scaledData)

// Show PCA Features
val result = pcaDF.select("pcaFeatures")
result.show()

// Use .head() to confirm that your output column Array of pcaFeatures
// only has 4 principal components
result.head(1)


////////////////////////
// Recommender System //
////////////////////////

//data: movei_ratings.csv
//task: build a recommender system.

import org.apache.spark.ml.recommendation.ALS

val ratings = spark.read.option("header","true").option("inferSchema","true").csv("movie_ratings.csv")

//split data into train test
val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

//build recommendation model (following documentation ex)
val als = new ALS().setMaxIter(5).setRegParam(0.01).setUserCol("userId").setItemCol("movieId").setRatingCol("rating")

val model = als.fit(training)

val predictions = model.transform(test)
//predictions.show() will show the output of the predictions

//How far are the predictions off from the ratings?
import org.apache.spark.sql.functions._
val error = predictions.select(abs($"rating"-$"prediction")) //take the abs value
//error.show()
//error.describe()  -> will have some NaNs since some userIDs that were not in both train and test set.
//instead, use error.na.drop().describe().show()
//On average, our model is off by a little less than 1 star.
