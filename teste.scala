// Databricks notebook source
// MAGIC %run ./DefaultSource

// COMMAND ----------

val df = spark
           .read
           .option("url", "https://gorest.co.in/public/v2/users")         
           .format("com.leivio.myrestconnector")
           .load("")
display(df)
