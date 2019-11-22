package com.mathartsys.yss.sparkmllib

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Matrices, Matrix}
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}

object MllibTest2 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\Tools\\Spark\\spark-2.1.0-bin-hadoop2.6\\data\\mllib\\iris.data")
    val conf = new SparkConf().setMaster("local[*]").setAppName("MllibTest")
    val sc = SparkContext.getOrCreate(conf)

    val observations=sc.textFile("G:/spark/iris.data")
      .map(_.split(","))
      .map(p => Vectors.dense(p(0).toDouble, p(1).toDouble, p(2).toDouble, p(3).toDouble))

    //print(observations.collect())
    val summary = Statistics.colStats(observations)
  }
}
