// Databricks notebook source
// MAGIC %run ./MicroServiceOptions

// COMMAND ----------

// MAGIC %run ./HttpClient

// COMMAND ----------

package com.leivio.myrestconnector

class CustomDatasourceRelation(override val sqlContext: org.apache.spark.sql.SQLContext,
                               val options: com.leivio.myrestconnector.MicroServiceOptions) extends org.apache.spark.sql.sources.BaseRelation with org.apache.spark.sql.sources.TableScan with Serializable {

import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

  val url = options.url
  val inferTimestamp = {if (options.inferTimestamp == "true") {true} else {false}}
  val pureDataset = {if (options.pureDataset == "true") {true} else {false}}

  import com.leivio.myrestconnector._

  def isEmptyOrNull(value: String) = value == null || value.trim.isEmpty

  def getPayload(): String = {

    val headers = Map(
      "Content-Type" -> "application/json"      
    )    

   HttpClient.get(url, headers)
  }

  def getRows(): IndexedSeq[String] = {
    import org.json._
    import collection.JavaConverters._
    val payload = getPayload()
    var arr =
      try {
        new JSONArray(payload)
      }
      catch {
        case e: JSONException => new JSONObject(payload).getJSONArray("content")
        case e: Throwable => throw e
      }
    (0 until arr.length).map(arr.getString)
  }

  val dsString = {
    import sqlContext.implicits._
    sqlContext.sparkContext.parallelize(getRows()).toDS()
  }

  val dfPersist =
    sqlContext
      .sparkSession
      .read
      .option("inferTimestamp", inferTimestamp)
      .json(dsString).persist(StorageLevel.MEMORY_ONLY)

  val pureSchema = StructType(Array(StructField("body", StringType, true)))

  override def schema: StructType = {
    if (pureDataset){
      pureSchema
    }
    else {
      dfPersist.schema
    }
  }

  override def buildScan(): RDD[Row] = {
    if (pureDataset){
      dsString.toDF().rdd
    }
    else {
      dfPersist.rdd
    }
  }

}
