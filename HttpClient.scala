// Databricks notebook source
package com.leivio.myrestconnector

object HttpClient extends Serializable {
  
  import org.apache.http.client.methods.HttpPost
  import org.apache.http.client.methods.HttpGet
  import org.apache.http.impl.client.CloseableHttpClient
  import org.apache.http.impl.client.HttpClients
  import org.apache.http.client.config.RequestConfig
  import org.apache.http.util.EntityUtils
  import org.apache.http.HttpEntity

   def get(url:String, headers: Map[String,String]): String = {    

    val request = new HttpGet(url)

    for ((key, value) <- headers) { 
      request.setHeader(key, value)
    }  

    val requestConfig = RequestConfig.custom()
                          .setConnectionRequestTimeout(15000)
                          .setConnectTimeout(15000)
                          .setSocketTimeout(5000)
                          .build();

    request.setConfig(requestConfig)

    val client = HttpClients.createDefault
    try {
      val response = client.execute(request)
      try {
        val statusCode = response.getStatusLine.getStatusCode
        val result = EntityUtils.toString(response.getEntity)
        if(statusCode == 200) {
          result
        }
        else {  
          throw new Exception(s"StatusCode: $statusCode\nError: $result")
        }        
      }
      finally {
        response.close
      }
    }
    finally {
      client.close
    }
  }
}
