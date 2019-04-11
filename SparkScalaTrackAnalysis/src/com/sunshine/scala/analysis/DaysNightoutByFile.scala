package com.sunshine.scala.analysis

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.text.SimpleDateFormat
import org.joda.time.DateTime
import org.apache.commons.lang.math.RandomUtils
import java.util.Properties
import org.apache.spark.sql.SaveMode


/**
 * 前提:按天创建创建过车轨迹文件
 * 基于每天文件进分行昼伏夜出初始化分值及过车轨迹次数
 * 
 * Local环境配置:
 * export HADOOP_HOME=E:\Bigdata\hadoop\hadoop-2.7.3
 * arguments:hdfs:10.2.20.11:8020/oy/data/hn_passrec_daysnightout.txt local
 * 
 * useage:
 * $>./spark-submit --class com.sunshine.scala.analysis.DaysNightoutObject SparkScalaTrackAnalysis-1.0.jar 
 * hdfs://bigdata1:8020/oy/data/hn/201606/hn_veh_passrec_20160621-30.txt yarn 5g 4g
 * 
 * 
 * Save analysis result data to mysql:
 * 
 * table:
 * create TABLE IF NOT EXISTS t_fx_daynightout_dayscore(
 *	hphm VARCHAR(10),
 *  hpzl VARCHAR(2),
 *  days VARCHAR(10),
 *  scores TINYINT
 * )ENGINE=InnoDB DEFAULT CHARSET=utf8
 * 
 * or
 * 
 * create TABLE IF NOT EXISTS t_fx_daynightout_dayscore (
 *  name VARCHAR(30), 
 *  score tinyint(4)
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8
 */
private[analysis] object DaysNightoutByFile {
  
  case class DaysNightHourDeatilScore(hphm: String, hpzl: String, day: String, hour: String, score: Int)
  case class DaysNightDayDeatilScore(hphm: String, hpzl: String, day: String, score: Int)
  case class DaysNightDayScore(name: String, score: Int)
  
  /**
   * Context Configuration
   * load values from any "spark.*" Java system properties set in your application
   */
  def config(args: Array[String]): SparkConf = {
     val conf = new SparkConf()
     .setMaster(args(0))
     .setAppName("DaysNightScore")
     .set("spark.deploy.mode", "client")
     .set("spark.driver.memory", args(1))
     .set("spark.executor.memory", args(2))
     return conf
  }
  
  def main(args: Array[String]): Unit = {
    
    var _file = ""
    var _master = "master"
    var _driverMemory="1g"
    var _executorMemory="1g"
    var _outFile="/"
    
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    //System.setProperty("HDP_VERSION", "2.6.1.0-129")
    
    if(args.length <= 0){
      println("DaysNightoutByFile UseAge: <InputFile> <Master> <DriverMemory> <ExecutorMemory> <OutPath>")
      System.exit(0)
    }
    if(args.length > 0) _file = args(0)
    if(args.length > 1) _master = args(1)
    if(args.length > 2) _driverMemory = args(2)
    if(args.length > 3) _executorMemory = args(3)
    
    val paras = Array(_master, _driverMemory, _executorMemory)
    
    // initialization Context
    val spark = SparkSession.builder().config(config(paras)).getOrCreate()

    val sparkContext = spark.sparkContext
   
    // Prepare file
    // max /oy/data/hn/201606/hn_veh_passrec_20160601-10.txt
    // min /oy/data/hn_passrec_daysnightout.txt
    val fileRdd = sparkContext.textFile(_file, 2) // RDD
    
    // Test view data
    //val df = spark.read.csv(_file); //DataFrame
    //df.show(10)
    //fileRdd.take(2).foreach(println)
    
    // Filter translator new rdd
    val scoreRdd = fileRdd.map(_.split(","))
                          .filter(filterHphmOrHpzl(_))
                          .map(initScore(_))
    // Combine
    // Tscore(counter, scores)
    type Tscore = (Int, Int)
    
    //(key_hour, counter, score_hour) 
    /*val comRdd = scoreRdd.combineByKey(score => (1, score), 
                                      (tscore: Tscore, nowScore) => (tscore._1 + 1, tscore._2 + nowScore),
                                      ((tscore1: Tscore), tscore2: Tscore) => ((tscore1._1 + tscore2._1), (tscore1._2 + tscore2._2)))
                                      .map{case (name, (counter, scores)) => (name, counter, scores/counter)}*/
    
    val comRdd = scoreRdd.combineByKey(score => (1, score), 
                                      (tscore: Tscore, nowScore) => (tscore._1 + 1, tscore._2),
                                      ((tscore1: Tscore), tscore2: Tscore) => ((tscore1._1 + tscore2._1), (tscore1._2)))                                  
                                      .map{ case (name, (counter, scores)) => (name, counter, scores)}          
    // Persist data
    comRdd.persist()
    
    import spark.implicits._
    
    // Split name translate new rdd
    val dayComRdd = comRdd.map{ case (name, counter, scores) => (name.substring(0, name.length()-2), scores)} 
    // Reduce by key
    val dayResComRdd = dayComRdd.reduceByKey((a1, a2) => a1 + a2)
    val dayResComDeatilRdd = dayResComRdd.map{case (name, scores) => DaysNightDayDeatilScore(name.split("_")(0), name.split("_")(1), name.split("_")(2), scores)}
    val dayResComDeatilDf = dayResComDeatilRdd.toDF()
    
    // View Partitions number
    //dayResComRdd.partitions.foreach(println)
    
    //val dayResComDataset = dayResComRdd.toDS().as[DaysNightDayScore] // why rdd->dataSet is Error? reason is dayResComRdd primitives is row
    //val dayResComDF = dayResComRdd.toDF("name","score") // rdd->dataFrames
    //val dayResComDataset = dayResComDF.as[DaysNightDayScore] // dataFrames->dataSet
    
    dayResComDeatilDf.show(50)
    
    // Save result to file
    //dayResComRdd.saveAsTextFile(_outFile.concat("/"+RandomUtils.nextLong()));
    
    // Save result to mysql databases and auto create table
    val connectionProperties = new Properties()
    connectionProperties.put("batchsize", "1000")
    connectionProperties.put("driver", "com.mysql.jdbc.Driver")
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "123456")
    dayResComDeatilDf
    .write.mode(SaveMode.Append).jdbc("jdbc:mysql://192.168.2.178:3306/vehicle_analysis?useUnicode=true&characterEncoding=utf8", //set character
        "t_daynightout_dayscore", connectionProperties)
    spark.stop()
  }
  
  /**
   * Filter hphm and hpzl is not recognized
   */
  def filterHphmOrHpzl(records: Array[String]): Boolean = {
    if (records(7).isEmpty() || records(7).equals("-") || "99".equals(records(6)))
      false
    else 
      true
  }
  
  /**
   * Score
   */
  def initScore(records: Array[String]): (String, Int) = {
    var score = 0
    val gcsj = records(3);
    val hour = new DateTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(gcsj).getTime).hourOfDay().get
    val key =  records(7).concat("_").concat(records(6))
                                     .concat("_").concat(gcsj.split(" ")(0))
                                     .concat("_").concat(String.valueOf(hour))
    hour match{
      case x if x>=0 && x<6 => score += 2
      case x if x>=6 && x<18 => score -= 1
      case x if x>=18 && x<23 => score += 0
      case x if x==23 => score += 2
    }  
    (key, score)
  }
}