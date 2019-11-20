package com.mathartsys.yss.sparkmllib

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Matrices, Matrix}
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}


object MllibTest {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\Tools\\Hadoop\\hadoop-2.6.0")
    val conf = new SparkConf().setMaster("local[*]").setAppName("MllibTest")
    val sc = SparkContext.getOrCreate(conf)

    //val dv: Vector = Vectors.dense(2.0, 0.0, 8.0)

    //val sv1: Vector = Vectors.sparse(3, Array(0, 2), Array(2.0, 8.0))

    //val sv2: Vector = Vectors.sparse(3, Seq((0, 2.0), (2, 8.0)))

    //val pos: LabeledPoint = LabeledPoint(1.0, Vectors.dense(2.0, 0.0, 8.0))

    //val neg: LabeledPoint = LabeledPoint(0.0, Vectors.sparse(3, Array(0, 2), Array(2.0, 8.0)))

    //val examples: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "D:\\Tools\\Spark\\spark-2.1.0-bin-hadoop2.6\\data\\mllib\\sample_libsvm_data.txt")

    //val dm: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))

    //val sm: Matrix = Matrices.sparse(3, 2, Array(0, 1, 3), Array(0, 2, 1), Array(9, 6, 8))

    val dv1 : Vector = Vectors.dense(1.0,2.0,3.0)
    val dv2 : Vector = Vectors.dense(2.0,3.0,4.0)
    val rows: RDD[Vector] = sc.parallelize(Array(dv1,dv2))
    val mat : RowMatrix = new RowMatrix(rows)
    val summary = mat.computeColumnSummaryStatistics()
    //println(summary.count)
    //最大向量
    //println(summary.max)
    //方差向量
    //println(summary.variance)
    //平均向量
    //println(summary.mean)
    //L1范数向量
    //println(summary.normL1)
    //L2范数向量
    //println(summary.normL2)

    val idxr1 = IndexedRow(1,dv1)
    val idxr2 = IndexedRow(2,dv2)

    val idxrows = sc.parallelize(Array(idxr1,idxr2))

    val idxmat = new IndexedRowMatrix(idxrows)

    //idxmat.rows.foreach(println)

    //坐标矩阵CoordinateMatrix是一个基于矩阵项构成的RDD的分布式矩阵。
    // 每一个矩阵项MatrixEntry都是一个三元组：
    // (i: Long, j: Long, value: Double)，其中i是行索引，j是列索引，value是该位置的值。
    // 坐标矩阵一般在矩阵的两个维度都很大，且矩阵非常稀疏的时候使用
    val ent1 = new MatrixEntry(0,1,0.5)
    val ent2 = new MatrixEntry(2,2,1.8)
    val entries: RDD[MatrixEntry] = sc.parallelize(Array(ent1,ent2))
    val coordMat = new CoordinateMatrix(entries)
    //coordMat.entries.foreach(println)
    //坐标矩阵可以通过transpose()方法对矩阵进行转置操作，
    // 并可以通过自带的toIndexedRowMatrix()方法转换成索引行矩阵IndexedRowMatrix
    val transMat = coordMat.transpose()
    //transMat.entries.foreach(println)
    val indexedRowMatrix = transMat.toIndexedRowMatrix()
    //indexedRowMatrix.rows.foreach(println)
    //分块矩阵
    val ent3 = new MatrixEntry(2,0,-1)
    val ent4 = new MatrixEntry(2,1,2)
    val ent5 = new MatrixEntry(2,2,1)
    val ent6 = new MatrixEntry(3,0,1)
    val ent7 = new MatrixEntry(3,1,1)
    val ent8 = new MatrixEntry(3,3,1)
    val entries1 = sc.parallelize(Array(ent1,ent2,ent3,ent4,ent5,ent6,ent7,ent8))
    //println(entries1.collect().foreach(println))
    val coordMat1 = new CoordinateMatrix(entries1)
    val matA = coordMat1.toBlockMatrix(2,2)
    matA.validate()
    println(matA.toLocalMatrix())
    val ata = matA.transpose.multiply(matA)
    println(ata.toLocalMatrix())
  }
}
