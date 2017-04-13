# Databricks notebook source
# MAGIC %md ## Spark SQL - Running Example
# MAGIC 
# MAGIC **Report By** : OUMOUSS EL MEHDI (M2 Data & Knowledge)

# COMMAND ----------

# MAGIC %md **Paper - Spark SQL: Relational Data Processing in Spark**
# MAGIC <p> https://pdfs.semanticscholar.org/f845/6b259bbf137ba89db548d77ab6643a2e40b2.pdf 

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ![About Spark](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/book_intro/spark_about.png)

# COMMAND ----------

# MAGIC %md **The SparkSession, which is going to be our access point to the Spark Framework:**

# COMMAND ----------

spark

# COMMAND ----------

# MAGIC %md ### ![Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/logo_spark_tiny.png) **DataFrames API**

# COMMAND ----------

# MAGIC %md **1. Using Semi-structed DataSet (Json) : **

# COMMAND ----------

df = spark.read.json('/FileStore/tables/rj2rp8ke1488295662051/moviepeople_1000-1c188.json')

# COMMAND ----------

df.show(5)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md Notice that the above cell takes 0.07 seconds to infer the schema by sampling the file and reading through it.
# MAGIC 
# MAGIC Inferring the schema works for ad hoc analysis against smaller datasets. But when working on multi-TB+ data, it's better to provide an **explicit pre-defined schema manually**, so there's no inferring cost.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType

# COMMAND ----------

# MAGIC %md Count how many rows total there are in DataFrame (and see how long it takes to do a full scan from remote disk):

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md **Q. show the first 5 people in the dataSet**

# COMMAND ----------

df.select('person-name').take(5)

# COMMAND ----------

# MAGIC %md **Q. The person named Anabela Teixeira**

# COMMAND ----------

df.filter(df['person-name']=='Teixeira, Anabela').show()

# COMMAND ----------

# MAGIC %md **Q. The birthplace of Steven Spielberg**

# COMMAND ----------

df.filter(df['person-name']=='Spielberg, Steven').select('info.birthdate', 'info.birthnotes').show()

# COMMAND ----------

# MAGIC %md **2. Using CSV File : Fire Safety Complaints**
# MAGIC 
# MAGIC Information on Complaints received by the Fire Department (from the public) for a particular location. Key fields include Complaint Number, Complaint Type, Address, Disposition
# MAGIC 
# MAGIC Dataset : https://data.sfgov.org/Housing-and-Buildings/Fire-Safety-Complaints/2wsq-7wmv 
# MAGIC 
# MAGIC API Doc: https://dev.socrata.com/foundry/data.sfgov.org/v3w9-dyka

# COMMAND ----------

df_csv = spark.read.load('/FileStore/tables/9rlemeo91488310038961/Fire_Safety_Complaints.csv',format='com.databricks.spark.csv', header='true', inferSchema='true')

# COMMAND ----------

df_csv.printSchema()

# COMMAND ----------

df_csv.columns

# COMMAND ----------

df_csv = df_csv\
.withColumnRenamed("Neighborhood  District", "NeighborhoodDistrict")\
.withColumnRenamed("Complaint Item Type Description", "ComplaintItemTypeDescription")\


# COMMAND ----------

# MAGIC %md **Q. How many complaints of each complaints type were there?**

# COMMAND ----------

display(df_csv.select('ComplaintItemTypeDescription').groupBy('ComplaintItemTypeDescription').count().orderBy("count", ascending=False))

# COMMAND ----------

# MAGIC %md The SF Fire department receive complaints from alarm systems more than any other type. 
# MAGIC 
# MAGIC Note that the above command took about 2.15 seconds to execute. 
# MAGIC In an upcoming section, we'll cache the data into memory for up to 100x speed increases.

# COMMAND ----------

from pyspark.sql.functions import *
from_pattern = 'dd/MM/yyyy'
to_pattern = 'yyyy-MM-dd'

df_csv = df_csv \
  .withColumn('ReceivedDate', unix_timestamp(df_csv['Received Date'], from_pattern).cast("timestamp")) \
  .drop('Received Date') \
  .withColumn('EntryDate', unix_timestamp(df_csv['Entry Date'], from_pattern).cast("timestamp")) \
  .drop('Entry Date')

# COMMAND ----------

display(df_csv)

# COMMAND ----------

df_csv.printSchema()

# COMMAND ----------

# MAGIC %md **Q. How many years of Fire Complaints is in the data file?**

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df_csv.select(year('ReceivedDate')).distinct().orderBy('year(ReceivedDate)').show()

# COMMAND ----------

# MAGIC %md **Q-4) How many complaints were received before Feb. 27th 2017 ?**

# COMMAND ----------

# MAGIC %md Note above that Feb 27th, 2017 is the 58th day of the year.

# COMMAND ----------

display(df_csv.filter(year('ReceivedDate') == '2017').filter(dayofyear('ReceivedDate') <= 58).groupBy(dayofyear('ReceivedDate')).count().orderBy('dayofyear(ReceivedDate)'))

# COMMAND ----------

# MAGIC %md ### ![Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/logo_spark_tiny.png) ** Memory and Caching **

# COMMAND ----------

# MAGIC %md The DataFrame is currently comprised of 2 partitions:

# COMMAND ----------

df_temp = df_csv
df_temp = df_temp.select('*', year('ReceivedDate').alias('ReceivedYear'))

# COMMAND ----------

df_temp.rdd.getNumPartitions()

# COMMAND ----------

df_temp.createOrReplaceTempView("df_VIEW");
df_temp.repartition(6).createOrReplaceTempView("df_VIEW");
spark.catalog.cacheTable("df_VIEW")

# COMMAND ----------

spark.table("df_VIEW").count()

# COMMAND ----------

fireComplaintsDF = spark.table("df_VIEW")
fireComplaintsDF.count()

# COMMAND ----------

# MAGIC %md Note that the full scan + count took less time (0.12 s compared to 0.78 seconds)

# COMMAND ----------

spark.catalog.isCached("df_VIEW")

# COMMAND ----------

# MAGIC %md ### ![Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/logo_spark_tiny.png) **SQL Queries**

# COMMAND ----------

# MAGIC %sql SELECT count(*) FROM df_VIEW;

# COMMAND ----------

# MAGIC %sql SELECT NeighborhoodDistrict, ReceivedYear FROM df_VIEW

# COMMAND ----------

# MAGIC %md **Q. Which neighborhood in SF generated the most calls this year?**

# COMMAND ----------

# MAGIC %sql SELECT NeighborhoodDistrict, count(NeighborhoodDistrict) AS Neighborhood_Count FROM df_VIEW WHERE ReceivedYear == '2017' GROUP BY NeighborhoodDistrict ORDER BY Neighborhood_Count DESC LIMIT 15

# COMMAND ----------

# MAGIC %md SQL also has some handy commands like `DESC` (describe) to see the schema + data types for the table:

# COMMAND ----------

# MAGIC %sql DESC df_VIEW;

# COMMAND ----------

# MAGIC %md ### ![Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/logo_spark_tiny.png) ** Spark Internals and SQL UI**

# COMMAND ----------

# MAGIC %md ![Catalyst](http://curriculum-release.s3-website-us-west-2.amazonaws.com/sf_open_data_meetup/catalyst.png)

# COMMAND ----------

# Note that a SQL Query just returns back a DataFrame
spark.sql("SELECT NeighborhoodDistrict, count(NeighborhoodDistrict) AS Neighborhood_Count FROM df_VIEW WHERE ReceivedYear == '2017' GROUP BY NeighborhoodDistrict ORDER BY Neighborhood_Count DESC LIMIT 15")

# COMMAND ----------

# MAGIC %md The `explain()` method can be called on a DataFrame to understand its logical + physical plans:

# COMMAND ----------

spark.sql("SELECT NeighborhoodDistrict, count(NeighborhoodDistrict) AS Neighborhood_Count FROM df_VIEW WHERE ReceivedYear == '2017' GROUP BY NeighborhoodDistrict ORDER BY Neighborhood_Count DESC LIMIT 15").explain(True)

# COMMAND ----------

# MAGIC %md We can view the visual representation of the SQL Query plan from the Spark UI.

# COMMAND ----------

# MAGIC %md ### ![Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/logo_spark_tiny.png) ** Spark MLlib **

# COMMAND ----------

# MAGIC %md ####1. Loading data

# COMMAND ----------

from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import DecisionTree
from pyspark.mllib.util import MLUtils

# Load data file 
data = MLUtils.loadLibSVMFile(sc, '/FileStore/tables/23phx7tu1475934034017/sample_libsvm_data.txt')
        # Split the data into training and test sets (40% held out for testing)
(trainingData, testData) = data.randomSplit([0.6, 0.4])

# COMMAND ----------

# MAGIC %md ####2. Training

# COMMAND ----------

# Train a DecisionTree model.
#  Empty categoricalFeaturesInfo indicates all features are continuous.
model = DecisionTree.trainClassifier(trainingData, numClasses=2, categoricalFeaturesInfo={},
                                     impurity='gini', maxDepth=5, maxBins=32)

# COMMAND ----------

# MAGIC %md ####3. Prediction & Evaluation 

# COMMAND ----------

# Evaluate model on test instances and compute test error
predictions = model.predict(testData.map(lambda x: x.features))
labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(testData.count())
print('Test Error = ' + str(testErr))
print('Learned classification tree model:')
print(model.toDebugString())

# COMMAND ----------

# MAGIC %md **References : **
# MAGIC 
# MAGIC 1) Spark 2.0 preview docs: https://people.apache.org/~pwendell/spark-nightly/spark-master-docs/
# MAGIC 
# MAGIC 2) DataFrame user documentation: https://people.apache.org/~pwendell/spark-nightly/spark-master-docs/latest/sql-programming-guide.html
# MAGIC 
# MAGIC 3) PySpark API 2.0 docs: https://people.apache.org/~pwendell/spark-nightly/spark-master-docs/latest/api/python/index.html
# MAGIC 
# MAGIC 4) Spark SQL : http://spark.apache.org/docs/latest/sql-programming-guide.html - http://spark.apache.org/sql/ 
