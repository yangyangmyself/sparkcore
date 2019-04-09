package com.sunshine.scala.analysis

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import java.util.UUID
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession

object FemaleInfoCollection {
  
  // path: /oy/data/tdata
  def main (args: Array[String]) {
     
    System.setProperty("HADOOP_USER_NAME", "hdfs");
    //System.setProperty("HADOOP_HOME", "/home/oy/hadoop-2.6.5");
    //System.setProperty("SPARK_HOME", "/home/oy/spark-2.2.0-bin-hadoop2.6");
    //System.setProperty("HADOOP_CONF_DIR", "/home/oy/hadoop-2.6.5/etc/hadoop");
    
    // Configure the Spark application name.
    // 2.2.1
    val spark = SparkSession
      .builder()
      .appName("CollectFemaleInfo")
      .master("local")
      .getOrCreate()
  
    // spark1.6.3
    /* 
    val conf = new SparkConf()
    conf.setAppName("CollectFemaleInfo")
    conf.set("spark.deploy.mode", "client")
    conf.setMaster("local") //yarn-client or local
    val sc:SparkContext = new SparkContext(conf)
    */  
      
    // Initializing Spark

    // Read data. This code indicates the data path that the input parameter args(0) specifies.
    val text = spark.sparkContext.textFile("/oy/data/tdata")
    
    // Filter the data information about the time that female netizens spend online.
    val data = text.filter(_.contains("female"))

    // Aggregate the time that each female netizen spends online
    val femaleData: RDD[(String, Int)] = data.map { line =>
      val t = line.split(',')
      (t(0), t(2).toInt)
    }.reduceByKey(_ + _)

    // Filter the information about female netizens who spend more than 2 hours online, and export the results
    val result = femaleData.filter(line => line._2 > 120)
    result.collect().map(x => x._1 + ',' + x._2).foreach(println)

    spark.stop()
  }
  
  def init(): SparkConf = {
    val sparkConf: SparkConf = new SparkConf(); 
    sparkConf.setMaster("yarn")
    //sparkConf.setMaster("yarn")
    sparkConf.set("deploy-mode", "client")
    sparkConf.set("spark.driver.maxResultSize", "1g"); 
    sparkConf.set("spark.driver.memory", "2g")
    sparkConf.setAppName("CollectFemaleInfo")
  }
}
