// Databricks notebook source
package com.leivio.myrestconnector

object MicroServiceOptions {

  import java.sql.{Connection, DriverManager}
  import java.util.{Locale, Properties}

  private val microServiceOptionNames = collection.mutable.Set[String]()

  private def newOption(name: String): String = {
    microServiceOptionNames += name.toLowerCase(Locale.ROOT)
    name
  }

  val REST_URL = newOption("url")
  val CONFIG_INFER_TIMESTAMP = newOption("inferTimestamp")
  val CONFIG_PURE_DATASET = newOption("pureDataset")
}

class MicroServiceOptions(@transient private val parameters: Map[String, String]) extends Serializable {
  import java.util.{Locale, Properties}
  import MicroServiceOptions._

  val asProperties: Properties = {
    val properties = new Properties()
    parameters.foreach { case (k, v) => properties.setProperty(k, v) }
    properties
  }

  require(parameters.isDefinedAt(MicroServiceOptions.REST_URL), s"Option '$REST_URL' is required.")  

  val url = parameters(MicroServiceOptions.REST_URL)
  // Option parameters 
  val inferTimestamp = parameters.getOrElse(MicroServiceOptions.CONFIG_INFER_TIMESTAMP, "true")
  val pureDataset = parameters.getOrElse(MicroServiceOptions.CONFIG_PURE_DATASET, "false")
}
