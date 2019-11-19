package com.mathartsys.yss.sparksql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

object ToHiveDemo01 {
  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("ToHiveDemo01")
    .enableHiveSupport()
    .getOrCreate()

  //老版本构建方法
  /*val conf = new SparkConf().setMaster("local[*]").setAppName("ToHiveDemo01")
  val sc = SparkContext.getOrCreate(conf)
  val sqlcontext: SQLContext = new HiveContext(sc)*/

  //1.添加驱动类
  val driver = "org.apache.hive.jdbc.HiveDriver"
  Class.forName(driver)




}
