package com.newyorker.data_engg.data_engg_smack

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

/**
 * @author ${user.name}
 */
object Executor {
	val conf = new SparkConf()
	.setAppName("Docker spark execution test")
	
	val sc = new SparkContext(conf)

  def main(args : Array[String]) {
    println("Calling loadJsonFile")
	  loadJsonFile()
	  println("Completed data loading ")

  }
  
  def loadJsonFile(){
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val jsonData = sqlContext.read.json("sample_twitter_data_json.json")
    jsonData.printSchema()
    jsonData.registerTempTable("jsonDat_table")
    val query1 = sqlContext.sql("select tweetmessage from jsonDat_table")
    println("RUNNING TAKE ON QUERY OBJECT: "+query1.take(1))

  }

}
