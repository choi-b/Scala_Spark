//Programmer: Brian Choi
//Date: 4.4.2020
//Spark DataFrames

///////////////
// OVERVIEW ///
///////////////

import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

// val df = spark.read.csv("CitiGroup2006_2008")

// add useful options
val df = spark.read.option("header","true").option("inferSchema","true").csv("CitiGroup2006_2008")

//df.head(5)

for(row <- df.head(5)){
  println(row)
}

df.columns //get column names

df.describe().show() //get summary of each column

df.select("Volume").show() //select a column

df.select($"Date",$"Close").show() //select multiple columns

val df2 = df.withColumn("HighPlusLow", df("High") + df("Low")) //create new columns
df2.printSchema()

df2.select(df2("HighPlusLow").as("HPL"),df2("Close")).show() //rename a column, and select multiple columns

//////////////////////////
// DataFrame Operations //
//////////////////////////

import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()
val df = spark.read.option("header","true").option("inferSchema","true").csv("CitiGroup2006_2008")
df.printSchema()

// ////////////////////////////////////
// /// Two main ways to filter out data
// /// 1. SparkSQL syntax
// /// 2. Scala syntax
// ////////////////////////////////////
//
//Scala Syntax
import spark.implicits._
df.filter($"Close" > 480).show()
//SQL syntax
df.filter("Close > 480").show()

// Multiple filters
df.filter($"Close" < 480 && $"High" < 480).show() // Scala syntax
df.filter("Close <480 AND High < 480").show() // SQL syntax

//Instead of showing the results, use collect to store it as a scala object
val CH_low = df.filter("Close <480 AND High < 480").collect() //saved as an array

//want to count how many rows?
val CH_low = df.filter("Close <480 AND High < 480").count()

//Filtering for equality (use triple === signs), double == signs will give you an error as of April 2020
df.filter($"High"===484.40).show()
df.filter("High = 484.40").show() //works with SQL Syntax

//Pearson correlation between two columns
df.select(corr("High", "Low")).show()


/////////////////////
// GroupBy and Agg //
/////////////////////

// GROUP BY and AGG (Aggregate methods)

// Start a simple Spark Session
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()

// Create a DataFrame from Spark Session read csv
// Technically known as class Dataset
val df = spark.read.option("header","true").option("inferSchema","true").csv("Sales.csv")

// Show Schema
df.printSchema()

// Show
df.show()

// Groupby Categorical Columns
// Optional, usually won't save to another object
df.groupBy("Company")

// Mean
df.groupBy("Company").mean().show()
// Count
df.groupBy("Company").count().show()
// Max
df.groupBy("Company").max().show()
// Min
df.groupBy("Company").min().show()
// Sum
df.groupBy("Company").sum().show()

// Other Aggregate Functions
// http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$
df.select(countDistinct("Sales")).show() //approxCountDistinct
df.select(sumDistinct("Sales")).show()
df.select(variance("Sales")).show()
df.select(stddev("Sales")).show() //avg,max,min,sum,stddev
df.select(collect_set("Sales")).show()

// OrderBy
// Ascending
df.orderBy("Sales").show()

// Descending
df.orderBy($"Sales".desc).show()


//////////////////
// Missing Data //
//////////////////

// Start a simple Spark Session
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()

// Grab small dataset with some missing data
val df = spark.read.option("header","true").option("inferSchema","true").csv("ContainsNull.csv")

// Show schema
df.printSchema()

// Notice the missing values!
df.show()

// 3 options with Null values
// 1. Keep them
// 2. Drop them
// 3. Fill them in with a value

// Drop any rows with any amount of na values
df.na.drop().show()

// Drop any rows that have less than a minimum Number of NON-null values ( < Int)
df.na.drop(2).show()

//using double/int versus strings
// Fill in all NA values with INT, but NOT STR
df.na.fill(100).show()

// Fill in String will only go to all string columns
df.na.fill("Missing Name").show()

// Be more specific, pass an array of string column names
df.na.fill("New Name", Array("Name")).show()

// Exercise: Fill in Sales with average sales.
df.describe().show()

// Now fill in with the values
df.na.fill(400.5).show()

//////////////////////////
// Dates and Timestamps //
//////////////////////////

// Spark Session
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()

// import data
val df = spark.read.option("header","true").option("inferSchema","true").csv("CitiGroup2006_2008")

// Show Schema
df.printSchema()

// For more options:
// http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$@add_months(startDate:org.apache.spark.sql.Column,numMonths:Int):org.apache.spark.sql.Column

df.select(month(df("Date"))).show()

df.select(year(df("Date"))).show()

// Practical Example
val df2 = df.withColumn("Year",year(df("Date")))

// Avg stock closing price PER YEAR
val dfavgs = df2.groupBy("Year").mean()
dfavgs.select($"Year",$"avg(Close)").show()

//see min instead
val dfmin = df2.groupBy("Year").min()
dfmin.select($"Year",$"min(Close)").show()

///////////////
// Exercises //
///////////////

//Use the Netflix_2011_2016.csv
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()
val df = spark.read.option("header","true").option("inferSchema","true").csv("Netflix_2011_2016.csv")

//column names
df.columns

//schema
df.printSchema()

//print out first 5 rows
df.head(5)

//describe()
df.describe().show()

//Create a new column of ratio of High price vs. volume of stock traded for a day
val df2 = df.withColumn("HV Ratio", df("High")/df("Volume"))
df2.columns
df2.head(1)

//what day had the peak high in price? order by the high price
df.orderBy($"High".desc).show(1)

//mean of close column
df.select(mean("Close")).show()

//max and min of volume
df.select(max("Volume")).show()
df.select(min("Volume")).show()

//for scala/spark syntax
import spark.implicits._

//how many days was the close lower than $600
df.filter($"Close"<600).count()
df.filter("Close < 600").count()

//what percent of the time was the high greater than 500
df.filter($"High">500).count()*1.0 / df.count() * 100 // *1.0 converts it to a double to get actual division

//pearson correlation between high and volume
df.select(corr("High","Volume")).show()

//What is the max high per year?
val yeardf = df.withColumn("Year", year(df("Date")))
val yearmaxs = yeardf.select($"Year", $"High").groupBy("Year").max()
yearmaxs.select($"Year",$"max(High)").show()

//order it by the year
val result = yearmaxs.select($"Year",$"max(High)").show()
result.orderBy("Year").show()

//Avg Close for each calendar month
val monthdf = df.withColumn("Month", month(df("Date")))
val monthavgs = monthdf.select($"Month", $"Close").groupBy("Month").mean()
monthavgs.select($"Month", $"avg(Close)").orderBy("Month").show()
