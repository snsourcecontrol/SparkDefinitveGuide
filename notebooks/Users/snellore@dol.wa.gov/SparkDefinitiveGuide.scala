// Databricks notebook source
val flightData2015 = spark  
.read  
.option("inferSchema", "true")  
.option("header", "true")  
.csv("/FileStore/tables/205_FlightData-30b2a.csv")

// COMMAND ----------

flightData2015.take(3)

// COMMAND ----------

flightData2015.sort("count").explain()

// COMMAND ----------

flightData2015.createOrReplaceTempView("flight_data_2015")

// COMMAND ----------

val sqlWay = spark.sql("""
SELECT DEST_COUNTRY_NAME, count(1)
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
""")

// COMMAND ----------

sqlWay.explain()

// COMMAND ----------

spark.sql("SELECT max(count) from flight_data_2015").take(1)

// COMMAND ----------

spark.sql("SELECT * from flight_data_2015").show()

// COMMAND ----------

val maxSql = spark.sql("""
SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
ORDER BY sum(count) DESC
LIMIT 5
""")

maxSql.show()

// COMMAND ----------

var df = spark.read.format("json").load("/FileStore/tables/2015_FlightData-ce878.json")

df.printSchema()

// COMMAND ----------

spark.read.format("json").load("/FileStore/tables/2015_FlightData-ce878.json").schema

// COMMAND ----------

import org.apache.spark.sql.types.{StructField,StructType,StringType,LongType}
import org.apache.spark.sql.types.Metadata

val myManualSchema = StructType(Array(
                               StructField("DEST_COUNTRY_NAME", StringType, true),
                               StructField("ORIGIN_COUNTRY_NAME", StringType, true),
                                StructField("count", LongType, false,
                                           Metadata.fromJson("{\"hello\":\"world\"}"))
                               ))

val df = spark.read.format("json").schema(myManualSchema)
.load("/FileStore/tables/2015_FlightData-ce878.json")

// COMMAND ----------

spark.read.format("json").load("/FileStore/tables/2015_FlightData-ce878.json").columns

// COMMAND ----------

val df = spark
  .read
.format("json")
.load("/FileStore/tables/2015_FlightData-ce878.json")

df.createOrReplaceTempView("dfTable")

// COMMAND ----------

// in Scala
df.show()

// COMMAND ----------

df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(2)

// COMMAND ----------

import org.apache.spark.sql.functions.{expr,col,column}

df.select(
  df.col("DEST_COUNTRY_NAME"),
  col("DEST_COUNTRY_NAME"),
  column("DEST_COUNTRY_NAME"),
  'DEST_COUNTRY_NAME,
  $"DEST_COUNTRY_NAME",
  expr("DEST_COUNTRY_NAME")
  ).show()

// COMMAND ----------

df.select(expr("DEST_COUNTRY_NAME as destination")).show(2)

// COMMAND ----------

df.select(expr("DEST_COUNTRY_NAME").alias("destination_country")).show()

// COMMAND ----------

df.selectExpr("DEST_COUNTRY_NAME as newcolumn", "DEST_COUNTRY_NAME").show(2)

// COMMAND ----------

// in Scala
df.selectExpr(
    "*", // include all original columns
    "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry")
  .show()

// COMMAND ----------

df.selectExpr("sum(count)", "count(*)", "count(distinct(DEST_COUNTRY_NAME))").show()

// COMMAND ----------

import org.apache.spark.sql.functions.lit
df.withColumn("withinCountry", expr("DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME")).show()

// COMMAND ----------

import org.apache.spark.sql.functions.expr

val dfWithLongColName = df.withColumn(
  "This Long Column-Name",
  expr("ORIGIN_COUNTRY_NAME"))

// COMMAND ----------

dfWithLongColName.show(2)

// COMMAND ----------

dfWithLongColName.selectExpr(
    "`This Long Column-Name`",
    "`This Long Column-Name` as `new col`")
  .show(2)

// COMMAND ----------

dfWithLongColName.createOrReplaceTempView("dfTableLong")

// COMMAND ----------

dfWithLongColName.select(col("This Long Column-Name")).columns

// COMMAND ----------

df.drop("ORIGIN_COUNTRY_NAME").columns

// COMMAND ----------

dfWithLongColName.drop("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").columns

// COMMAND ----------

df.withColumn("count2", col("count").cast("long"))

// COMMAND ----------

df.filter(col("count")<20).show()

// COMMAND ----------

df.select("ORIGIN_COUNTRY_NAME","DEST_COUNTRY_NAME").distinct.show()

// COMMAND ----------

val seed = 5
val withReplacement = false
val fraction = 0.5
df.sample(withReplacement, fraction, seed).count()

// COMMAND ----------

val dataFrames = df.randomSplit(Array(0.25,0.75),seed)

dataFrames(0).count > dataFrames(1).count


// COMMAND ----------

df.rdd.getNumPartitions

// COMMAND ----------

df.repartition(5)

// COMMAND ----------

df.repartition(col("DEST_COUNTRY_NAME"))

// COMMAND ----------

df.repartition(5,col("DEST_COUNTRY_NAME"))

// COMMAND ----------

df.repartition(5,col("DEST_COUNTRY_NAME")).coalesce(2)

// COMMAND ----------

// in Scala
val collectDF = df.limit(10)
collectDF.collect()

// COMMAND ----------

collectDF.toLocalIterator()

// COMMAND ----------

val df = spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
.load("/FileStore/tables/2010_12_01-ec65d.csv")

df.printSchema()

df.createOrReplaceTempView("dfTable")

// COMMAND ----------

import org.apache.spark.sql.functions

df.select(lit(5),lit("five"),lit(5.0))

// COMMAND ----------

import org.apache.spark.sql.functions

df.where(col("InvoiceNo").equalTo(536365))
.select("InvoiceNo", "Description")
.show(5, false)

// COMMAND ----------

import org.apache.spark.sql.functions

df.where(col("InvoiceNo") =!= 536365)
.select("InvoiceNo", "Description")
.show(5, false)

// COMMAND ----------

df.where("InvoiceNo = 536365")
.show(5, false)

// COMMAND ----------

val PriceFilter = col("UnitPrice") > 600
val descripFilter = col("Description").contains("POSTAGE")

df.where(col("stockCode").isin("DOT")).where(PriceFilter.or(descripFilter))
.show()

// COMMAND ----------

val DOTCodeFilter = col("StockCode") === "DOT"
val priceFilter = col("UnitPrice") < 250
val descripFilter = col("Description").contains("POSTAGE")

df.withColumn("isExpensive", DOTCodeFilter.and(priceFilter.or(descripFilter)))
.where("isExpensive")
.select("UnitPrice", "isExpensive")
.show(5, false)

// COMMAND ----------

import org.apache.spark.sql.functions.{expr,pow}

val fabricatedquantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5

df.select(expr("CustomerID"), fabricatedquantity.alias("realQuantity")).show(2)

// COMMAND ----------

import org.apache.spark.sql.functions.{round,bround}
df.select(round(col("UnitPrice"), 1).alias("rounded"), col("UnitPrice")).show(5)

// COMMAND ----------

import org.apache.spark.sql.functions.lit

df.select(round(lit("2.5")), bround(lit("2.5"))
         ).show(2)

// COMMAND ----------

import org.apache.spark.sql.functions.corr

df.stat.corr("Quantity", "UnitPrice")


// COMMAND ----------

df.select(corr("Quantity", "UnitPrice")).show()

// COMMAND ----------

df.describe().show()

// COMMAND ----------

import org.apache.spark.sql.functions.monotonically_increasing_id

df.select(monotonically_increasing_id()).show(10)

// COMMAND ----------

val colName = "UnitPrice"
val quantileprobs = Array(0.5)
val relError = 0.005

df.stat.approxQuantile("UnitPrice", quantileprobs, relError)

// COMMAND ----------

df.stat.crosstab("StockCode","Quantity").show()

// COMMAND ----------

import org.apache.spark.sql.functions.regexp_replace

val simpleColors = Seq("black", "white", "red", "green", "blue")
val regexString = simpleColors.map(_.toUpperCase).mkString("|")

df.select(col("Description"),
  regexp_replace(col("Description"), regexString, "COLOR").alias("color_clean")
).show(10, false)

// COMMAND ----------

import org.apache.spark.sql.functions.{current_date,current_timestamp}

val dateDF = spark.range(10)
.withColumn("today", current_date())
.withColumn("now", current_timestamp())

dateDF.createOrReplaceTempView("dateTable")

// COMMAND ----------

dateDF.printSchema()

// COMMAND ----------

dateDF.show(false)

// COMMAND ----------

import org.apache.spark.sql.functions.{date_add,date_sub}

dateDF.select(date_sub(col("today"),5), date_add(col("today"), 5)).show()

// COMMAND ----------

var df = spark.read.json("/FileStore/tables/simple_ml-a9b93.json")
df.orderBy("value2").show()

// COMMAND ----------

import org.apache.spark.ml.feature.RFormula
val supervised = new RFormula()
.setFormula("lab ~. + color:value1 + color:value2")

// COMMAND ----------

val fittedRF = supervised.fit(df)
val preparedDF = fittedRF.transform(df)
preparedDF.show(false)

// COMMAND ----------

val Array(train, test) = preparedDF.randomSplit(Array(0.7,0.3)) 

// COMMAND ----------

import org.apache.spark.ml.classification.LogisticRegression
val lr = new LogisticRegression().setLabelCol("label").setFeaturesCol("features")

// COMMAND ----------

println(lr.explainParams)

// COMMAND ----------

val fittedLR = lr.fit(train)

// COMMAND ----------

fittedLR.transform(train).select("label","prediction").show()

// COMMAND ----------

val Array(train,test) = df.randomSplit(Array(0.7,0.3))

// COMMAND ----------

val rForm = new RFormula()
val lr = new LogisticRegression().setLabelCol("label").setFeaturesCol("features")

// COMMAND ----------

import org.apache.spark.ml.Pipeline
val stages = Array(rForm, lr)
val pipeline = new Pipeline().setStages(stages)

// COMMAND ----------

import org.apache.spark.ml.tuning.ParamGridBuilder
val params = new ParamGridBuilder()
  .addGrid(rForm.formula, Array(
    "lab ~. + color:value1",
    "lab ~. + color:value2"))
  .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
  .addGrid(lr.regParam, Array(0.1, 2.0))
  .build()

// COMMAND ----------

import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
val evaluator = new BinaryClassificationEvaluator()
  .setMetricName("areadUnderROC")
  .setRawPredictionCol("prediction")
  .setLabelCol("label")

// COMMAND ----------

