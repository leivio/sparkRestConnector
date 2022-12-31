// Databricks notebook source
// MAGIC %run ./CustomDatasourceRelation

// COMMAND ----------

package com.leivio.myrestconnector

class DefaultSource extends org.apache.spark.sql.sources.RelationProvider with org.apache.spark.sql.sources.DataSourceRegister {

import org.apache.spark.sql.sources.{BaseRelation}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType

  override def shortName(): String = "myrest"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val microServiceOptions = new MicroServiceOptions(parameters)
    import com.leivio.myrestconnector._
    new CustomDatasourceRelation(sqlContext, microServiceOptions)
  }
}
