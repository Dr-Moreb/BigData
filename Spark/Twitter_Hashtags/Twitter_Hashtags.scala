// Databricks notebook source exported at Sun, 23 Oct 2016 18:20:44 UTC
import sys.process._
"wget -P /tmp https://www.datacrucis.com/media/datasets/stratahadoop-BCN-2014.json" !!

// COMMAND ----------

val localpath="file:/tmp/stratahadoop-BCN-2014.json"
dbutils.fs.mkdirs("dbfs:/datasets/")
dbutils.fs.cp(localpath, "dbfs:/datasets/")
display(dbutils.fs.ls("dbfs:/datasets/stratahadoop-BCN-2014.json"))
val df = sqlContext.read.json("dbfs:/datasets/stratahadoop-BCN-2014.json")

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext._

// COMMAND ----------

df.printSchema

// COMMAND ----------

//Get an RDD with the text of the tweets
val rdd = df.select("text").rdd.map(row => row.getString(0))

// COMMAND ----------

//Count words
val wordCounts = rdd.flatMap(_.split(" ")).map(word =>(word,1)).reduceByKey((a,b) => a+b)

// COMMAND ----------

val words = rdd.flatMap(word => word.split(" "))
val hashTags = words.filter(word => word.startsWith("#"))
hashTags.take(10).foreach(println)

// COMMAND ----------

val nbHashtags = hashTags.count()
//8375

// COMMAND ----------

val frequentHashTags = hashTags.flatMap(_.split(" ")).map(word =>(word,1)).reduceByKey((a,b) => a+b)
val sortedHashTags = frequentHashTags.map(_ swap).sortByKey(false)
sortedHashTags.take(10).foreach(println)
frequentHashTags.count()

// COMMAND ----------

val users = df.select($"user.name")
val userCounts = users.rdd.map(row => row.getString(0)).map(word =>(word,1)).reduceByKey((a,b) => a+b)
val sortedUsers = userCounts.sortBy(-_._2)
sortedUsers.take(10).foreach(println)


// COMMAND ----------

/*A trend on Twitter refers to a hashtag-driven topic that is
immediately popular at a particular time
Here, I am trying to output the most popular hashtags per day.
*/

// COMMAND ----------

//dfTmpx : df_date_to_hashtags
val df_date_to_hashtags = df.select($"created_at",explode($"entities.hashtags.text").as("schools_flat")).sort($"created_at".desc) 
df_date_to_hashtags.take(10).foreach(println)


// COMMAND ----------

def NormalizedDate(date: String) : Int = 
 return date.substring(8,10).toInt

val df_normalized = df_date_to_hashtags.map(row => {
    val c_date = row.getAs[String](0)
    val n_date = NormalizedDate(c_date)
    ((n_date,row(1).toString()),1)
  })
df_normalized.take(10).foreach(println)

// COMMAND ----------

val df_reduced = df_normalized.reduceByKey(_+_)
val df_grouped = df_reduced.map{case((x,y),z) => (x,(y,z))}.groupByKey()
df_grouped.take(10).foreach(println)

// COMMAND ----------

val df_sorted = df_grouped.map{case(x,y) => (x, y.toList.sortBy(-_._2))}
df_sorted.take(2).foreach(println)

// COMMAND ----------

val df_result = df_sorted.map{case(x,y) => (x, y.take(5))}
df_result.collect.foreach(println)
