package com.mathartsys.yss.sparkmllib

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Matrix, Matrices}


object MllibTest {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\Tools\\Hadoop\\hadoop-2.6.0")
    val conf = new SparkConf().setMaster("local[*]").setAppName("MllibTest")
    val sc = SparkContext.getOrCreate(conf)

    val dv: Vector = Vectors.dense(2.0, 0.0, 8.0)

    val sv1: Vector = Vectors.sparse(3, Array(0, 2), Array(2.0, 8.0))

    val sv2: Vector = Vectors.sparse(3, Seq((0, 2.0), (2, 8.0)))

    val pos: LabeledPoint = LabeledPoint(1.0, Vectors.dense(2.0, 0.0, 8.0))

    val neg: LabeledPoint = LabeledPoint(0.0, Vectors.sparse(3, Array(0, 2), Array(2.0, 8.0)))

    val examples: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "D:\\Tools\\Spark\\spark-2.1.0-bin-hadoop2.6\\data\\mllib\\sample_libsvm_data.txt")

    val dm: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))

    val sm: Matrix = Matrices.sparse(3, 2, Array(0, 1, 3), Array(0, 2, 1), Array(9, 6, 8))

    print(sm)
  }
}
