package com.mathartsys.yss.sparksql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SaveMode
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._

object CodeRuleScala {
  //scala方法定义
  //验证值是否有效方法
  def if_null(x:Any):Integer={
    if(x!=""&&x!="null"&&x!=null) 1 else 2
  }

  //验证时间是否在某个月份
  def if_in_months_o(date:String,diff:Int):Integer={
    val result = if_in_months(date,diff)
    if(result==2||result==null){
      return 0
    }
    return result
  }

  //判断日期是否在3个月（6个月，12个月，一周）内。。。过去一周和未来一周内，diff分别用-13和13表示，过去xx月内和未来xx月内，
  // diff分别用-xx和xx表示,是为1，否为2，异常为null
  def if_in_months(date:String,diff:Int):Integer={
    if(if_null(date)==1){
      val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val cal = Calendar.getInstance()
      val cal1 = Calendar.getInstance()
      val dt = sdf.parse(date)
      cal1.setTime(dt)
      cal.setTime(sdf.parse(sdf.format(new Date())))
      cal.add(Calendar.MONTH, diff)
      cal.add(Calendar.DAY_OF_MONTH,-diff/Math.abs(diff))
      if(dt.getTime <= (new Date).getTime && cal1.getTimeInMillis >= cal.getTimeInMillis) 1 else if(dt.getTime >= (new Date).getTime && cal1.getTimeInMillis <= cal.getTimeInMillis) 1 else 2
    }else null
  }

  //spark主调用程序
  def main(args: Array[String]): Unit = {
    val tagName = args(0) //shell 脚本参数1
    val sqlPath = args(1) // shell 脚本参数2
    val conf = new SparkConf().setAppName("SparkDmpTag") //spark程序名定义
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc) //hiveContext包初始化

    //如果不追加以下两句配置信息,以partition的方式存表会报错
    //enable hive dynamic partition
    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    //strict to prevent the all-dynamic partition case
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    //从hdfs路径中读取hive sql语句解析成可识别的string
    val sql =sc.textFile(sqlPath + "/" + tagName+".sql")
      .map(x=>{x.replaceAll("(^[\\s]*--[\\s\\S]*)","").replaceAll("(--[^s]+)"," ")})
      .toLocalIterator.mkString(" ")

    //将scala定义的方法注册成udf方法
    sqlContext.udf.register("if_in_months_o",if_in_months_o _)
    sqlContext.udf.register("if_null", if_null _)
    sqlContext.udf.register("if_in_months",if_in_months _)

    //获取当天的时间格式
    val now:Date = new Date()
    val dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val dt = dateFormat.format(now)

    //根据hive sql生成目标分析表
    val result = sqlContext.sql(sql)
    //追加partition dt字段
    val result_dt = result.withColumn("dt",lit(dt))
    //将追加好dt分区字段的dataframe分区存储到建好的hive表中
    result_dt.write.mode(SaveMode.Overwrite).partitionBy("dt").insertInto("dmp_tags"+"."+tagName)
  }

}
