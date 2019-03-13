// Databricks notebook source
// MAGIC %md
// MAGIC #### Q2 - Skeleton Scala Notebook
// MAGIC This template Scala Notebook is provided to provide a basic setup for reading in / writing out the graph file and help you get started with Scala.  Clicking 'Run All' above will execute all commands in the notebook and output a file 'toygraph.csv'.  See assignment instructions on how to to retrieve this file. You may modify the notebook below the 'Cmd2' block as necessary.
// MAGIC 
// MAGIC #### Precedence of Instruction
// MAGIC The examples provided herein are intended to be more didactic in nature to get you up to speed w/ Scala.  However, should the HW assignment instructions diverge from the content in this notebook, by incident of revision or otherwise, the HW assignment instructions shall always take precedence.  Do not rely solely on the instructions within this notebook as the final authority of the requisite deliverables prior to submitting this assignment.  Usage of this notebook implicitly guarantees that you understand the risks of using this template code. 

// COMMAND ----------

/*
DO NOT MODIFY THIS BLOCK
This assignment can be completely accomplished with the following includes and case class.
Do not modify the %language prefixes, only use Scala code within this notebook.  The auto-grader will check for instances of <%some-other-lang>, e.g., %python
*/
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions._
case class edges(Source: String, Target: String, Weight: Int)
import spark.implicits._

// COMMAND ----------

/* 
Create an RDD of graph objects from our toygraph.csv file, convert it to a Dataframe
Replace the 'toygraph.csv' below with the name of Q2 graph file.
*/

val df = spark.read.textFile("/FileStore/tables/bitcoinalpha.csv") 
  .map(_.split(","))
  .map(columns => edges(columns(0), columns(1), columns(2).toInt)).toDF()

//val nRow = Seq((3, 1, 4))
//val appe = df.union(nRow.toDF())

// COMMAND ----------

// Insert blocks as needed to further process your graph, the division and number of code blocks is at your discretion.

// COMMAND ----------

// e.g. eliminate duplicate rows
val df1 = df.dropDuplicates()
df1.show(3)

// COMMAND ----------

// e.g. filter nodes by edge weight >= supplied threshold in assignment instructions
val df2 = df1.filter($"Weight" >= 5)
df2.show(3)

// COMMAND ----------

val df3 = df2.groupBy("Target").agg(count("*")).toDF("Target","In")

val df4 = df3.join(df2.groupBy("Source").agg(count("*")), df3.col("Target") === df2.col("Source"), joinType="left").toDF("node","in-degree","Source","out-degree").drop("Source").na.fill(0, Seq("out-degree")).withColumn("total-degree", List(col("in-degree"),col("out-degree")).reduce(_ + _)).select("node","out-degree","in-degree","total-degree").orderBy(asc("node"))

df4.show()

// find node with highest in-degree, if two or more nodes have the same in-degree, report the one with the lowest node id
df4.orderBy(desc("in-degree"), asc("node")).limit(1).select("node").show()

// find node with highest out-degree, if two or more nodes have the same out-degree, report the one with the lowest node id
df4.orderBy(desc("out-degree"), asc("node")).limit(1).select("node").show()

// find node with highest total degree, if two or more nodes have the same total degree, report the one with the lowest node id
df4.orderBy(desc("total-degree"), asc("node")).limit(1).select("node").show()

/*
// Show the full list using this code
val maxInDeg = df4.agg(max("in-degree")).select("max(in-degree)").first().getLong(0).toInt
df4.filter($"in-degree" === maxInDeg).select("node").show()

// find node with highest out-degree, if two or more nodes have the same out-degree, report the one with the lowest node id
val maxOutDeg = df4.agg(max("out-degree")).select("max(out-degree)").first().getLong(0).toInt
df4.filter($"out-degree" === maxOutDeg).select("node").show()

// find node with highest total degree, if two or more nodes have the same total degree, report the one with the lowest node id
val maxTotDeg = df4.agg(max("total-degree")).select("max(total-degree)").first().getLong(0).toInt
df4.filter($"total-degree" === maxTotDeg).select("node").show()
*/

// COMMAND ----------

/*
Create a dataframe to store your results
Schema: 3 columns, named: 'v', 'd', 'c' where:
'v' : vertex id
'd' : degree calculation (an integer value.  one row with highest in-degree, a row w/ highest out-degree, a row w/ highest total degree )
'c' : category of degree, containing one of three string values:
                                                'i' : in-degree
                                                'o' : out-degree                                                
                                                't' : total-degree
- Your output should contain exactly three rows.  
- Your output should contain exactly the column order specified.
- The order of rows does not matter.
                                                
A correct output would be:

v,d,c
2,3,i
1,3,o
2,6,t


whereas:
- Node 1 has highest in-degree with a value of 3
- Node 2 has highest out-degree with a value of 3
- Node 2 has highest total degree with a value of 6
*/

val df5 = df4.orderBy(desc("in-degree"), asc("node")).limit(1).select("node","in-degree").withColumnRenamed("in-degree","d").withColumnRenamed("node","v").withColumn("c", lit("i")).union(
df4.orderBy(desc("out-degree"), asc("node")).limit(1).select("node","out-degree").withColumnRenamed("out-degree","d").withColumnRenamed("node","v").withColumn("c", lit("o"))).union(
df4.orderBy(desc("total-degree"), asc("node")).limit(1).select("node","total-degree").withColumnRenamed("total-degree","d").withColumnRenamed("node","v").withColumn("c", lit("t")))

df5.show()

// COMMAND ----------

display(df5)

// COMMAND ----------


