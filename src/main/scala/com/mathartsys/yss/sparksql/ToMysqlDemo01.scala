package com.mathartsys.yss.sparksql

import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object  ToMysqlDemo01 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("ToMysqlDemo01")
      .getOrCreate()

    //val sc: SparkContext = spark.sparkContext

    val url = "jdbc:mysql://10.0.9.3:3306/salika"
    val table = "actor"
    val prop = new Properties()
    prop.put("user","root")
    prop.put("password","123456")
    val df = spark.read.jdbc(url,table,prop)
    //df.show(20)

  }
}
