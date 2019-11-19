package com.mathartsys.yss

import org.apache.spark._
object SparkDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\Tools\\Hadoop\\hadoop-2.6.0")
    val masterUrl = "local[1]"
    val conf = new SparkConf().setAppName("helenApp").setMaster(masterUrl)
    val sc = new SparkContext(conf)


    val rdd = sc.parallelize(List(1, 2, 3, 4, 5, 6)).map(_ * 3)

    rdd.filter(_ > 10).collect().foreach(println)
    println(rdd.reduce(_ + _))

    println("hello world")
  }
}
