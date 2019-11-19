package com.mathartsys.yss.sparkstreaming

import java.sql.DriverManager
import java.util.Properties
import util.DateUtils
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by admin on 2019/8/7.
  */
object ADRealTimeANA {
  def main(args: Array[String]): Unit = {
    //构建上下文
    val conf = new SparkConf().setMaster("local[*]").setAppName("ADRealTimeANA")
    val sc = SparkContext.getOrCreate(conf)
    val ssc = new StreamingContext(sc,Seconds(10))

    //设置checkpoint
    val cppath = s"result/checkpoint/ad_${System.currentTimeMillis()}"
    ssc.checkpoint(cppath)

    //创建Dstream流
    val kafkaParams = Map(
      "zookeeper.connect"->"boduoyejieyi:2181/adlog",
      "group.id"->"sparkstreaming",
      "auto.offset.reset"->"smallest"
    )
    val topics = Map(
      "adlog"->2
    )
    val storageLevel = StorageLevel.MEMORY_AND_DISK
    val dstream = KafkaUtils
      .createStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics,storageLevel)
      .map(t=>t._2)

    /*
    根据日志的url不同来区分是否是广告流量
	  已使用flume的正则拦截器来进行分流
     */

    /*
    广告流量数据的原始格式（String）：
        timestamp:日志产生的时间戳
        province：省份
        city：城市
        userid：用户的id
        adid：广告id
     */

    //3.1数据格式转换
    val formatdstream = this.formatAdRealTimeDstream(dstream)

    //3.2根据数据库中已有黑名单对实时数据过滤
    val filteredByBlackUserDstream: DStream[AdClickRecord] = this.filterByBlackUser(formatdstream)

    //3.3求每个用户最近5分钟点击次数，大于100次则为新黑名单 WINDOW
    this.dynamicUpdateBlackUser(filteredByBlackUserDstream)

    //3.4实时统计每天每个省份每个城市广告点击量
    val aggds: DStream[((String, String, String, Int), Int)] = this.DayProvinceCityADCount(filteredByBlackUserDstream)

    //3.5统计每天每个省份top5的广告
    this.DayProvinceCityADTop5(aggds)

    //3.6计算最近一段时间的广告的点击次数 WINDOW
    this.AdClickWindow(filteredByBlackUserDstream)

    ssc.start()
    ssc.awaitTermination()
  }

  //定义方法
  //3.1数据格式转换方法
  def formatAdRealTimeDstream(dstream:DStream[String]): DStream[AdClickRecord] ={
    dstream.map(line=>line.split(" ")).map(arr=>{
      AdClickRecord(arr(0).toLong,arr(1),arr(2),arr(3).toInt,arr(4).toInt)
    })
  }

  //3.2根据数据库中已有黑名单对实时数据过滤方法
  def filterByBlackUser(dstream:DStream[AdClickRecord]): DStream[AdClickRecord] ={
    dstream.transform(rdd=>{
      //把黑名单从数据库中读出来
      val blackListRDD: RDD[(Int, Int)] = {
        val sc = rdd.sparkContext
        val url = ""
        val table = ""
        val prop = new Properties()
        prop.put("user","root")
        prop.put("password","123456")

        val sqlContext = new SQLContext(sc)
        sqlContext.read.jdbc(url,table,prop).rdd.map(row=>{
          val user_id = row.getAs[Int]("user_id")
          val count = row.getAs[Int]("count")
          (user_id,count)
        })
      }
      //基于黑名单对实时用户进行判断
      //rdd是当前实时数据
      rdd.map(record=>(record.userid,record))
        .leftOuterJoin(blackListRDD)
        .filter{//返回true保留
          case(_,(_,count))=>count.isEmpty//count为空表示该userid不在黑名单表
        }.map{
        case(_,(record,_))=>record
      }
    })
  }

  //3.3求每个用户最近5分钟点击次数，大于100次则为新黑名单方法
  def dynamicUpdateBlackUser(dstream:DStream[AdClickRecord]) ={
    //计算用户最近5分钟点击次数
    val dd = dstream.map(record=>(record.userid,1)).reduceByKeyAndWindow(
      (a,b)=>a+b,
      (c,d)=>c-d,
      Minutes(5),//最近5分钟
      Seconds(30)//每30秒看一次
    )
    //判断当前用户点击次数是否大于100，若大于100加入黑名单
    dd.filter(t=>t._2>100)
      .foreachRDD(rdd=>{
        rdd.foreachPartition(item=>{
          item.foreach(t=>{
            //获取数据库连接
            val url = ""
            val user = "root"
            val password = "123456"
            val conn = DriverManager.getConnection(url,user,password)

            val sql = "insert into tb_black_users values(?,?)"

            val pstmt = conn.prepareStatement(sql)

            pstmt.setInt(1,t._1)
            pstmt.setInt(2,t._2)

            pstmt.executeUpdate()

            pstmt.close()
            conn.close()
          })
        })
      })
  }

  //3.4实时统计每天每个省份每个城市广告点击量方法
  def DayProvinceCityADCount(dstream:DStream[AdClickRecord])={
    //AdClickRecord(timestamp,province,city,userid,adid)
    val tmpds = dstream.map{
      case AdClickRecord(timestamp,province,city,userid,adid) => {
        val date = DateUtils.parseLong2String(timestamp, "yyyy-MM-dd")
        ((date,province,city,adid),1)
      }
    }//tmpds为(key,1)类型，相当于wordcount

    val aggds: DStream[((String, String, String, Int), Int)] = tmpds.reduceByKey(_ + _)//可理解为wordcount
      .updateStateByKey(
      (seq:Seq[Int],state:Option[Int])=>{
        val currentValue = seq.sum
        val preValue = state.getOrElse(0)
        Some(currentValue+preValue)
      }
    )//使用updateStateByKey实现累计

    //结果写入数据库
    aggds.foreachRDD(rdd=>{
      val sc = rdd.sparkContext
      val sqlContext = new SQLContext(sc)

      import sqlContext.implicits._

      val url = ""
      val table = ""
      val prop = new Properties()
      prop.put("user","root")
      prop.put("password","123456")

      //此时rdd类型为[((String, String, String, Int), Int)] (和aggds一样)，需用map先进行转化
      val df = rdd.map(t=>(t._1._1,t._1._2,t._1._3,t._1._4,t._2))
        .toDF("date","province","city","adid","count").write.mode(SaveMode.Append)//追加方式写入
        .jdbc(url,table,prop)
    })
    aggds
  }

  //3.5统计每天每个省份top5的广告点击量方法
  // (已求得每天每个省份每个城市广告点击量)
  def DayProvinceCityADTop5(dstream: DStream[((String, String, String, Int), Int)]) ={
    val dailyProvinceADCount = dstream.map{
      case ((date,province,city,adid),count) => ((date,province,adid),count) //先把不需要的字段city去掉
    }.reduceByKey((a,b)=>a+b)
    //去掉一个字段后肯定要重新聚合一下

    //根据以上结果求top5
    val top5count: DStream[(String, String, Int, Int)] = dailyProvinceADCount.map{
      case((date,province,adid),count)=>((date,province),(adid,count))//将两个分组字段date,province单独取出作为key
    }.groupByKey()//根据两个分组字段date,province先分组，每组内为某天某省的所有广告的点击次数，需要求出点击数前5的广告id
      .flatMap{
      case((date,province),iter)=>{// 分组后(adid,count)变为迭代器类型
      val top5: List[(Int, Int)] = iter.toList.sortBy(t=>t._2).takeRight(5)//迭代器求top常用方法
        top5.map{
          case(adid,count)=>(date,province,adid,count)//※扁平化，把date,province加回去，得到top5结果
        }
      }
    }
    //结果写入数据库
    top5count.foreachRDD(rdd=>{
      val sc = rdd.sparkContext
      val sqlContext = new SQLContext(sc)

      import  sqlContext.implicits._

      val url = ""
      val table = ""
      val prop = new Properties()
      prop.put("user","root")
      prop.put("password","123456")

      val df = rdd.toDF("date","province","adid","count").write.mode(SaveMode.Overwrite).jdbc(url,table,prop)
    })
    top5count
  }

  //3.6计算最近一段时间的广告的点击次数方法
  def AdClickWindow (dstream:DStream[AdClickRecord]) ={
    val userdstream: DStream[(Int, Int)] = dstream.map(record=>(record.userid,1))

    val aggregateuserdstream: DStream[(Int, Int)] = userdstream.reduceByKeyAndWindow(
      (a,b) => a+b,
      (c,d) => c-d,
      Minutes(10),//近10分钟
      Minutes(1)//每1分钟打印一次
    )

    //加入时间维度（时间戳）
    val finaldstream = aggregateuserdstream.transform((rdd,time)=>{
      val timestr = DateUtils.parseLong2String(time.milliseconds,"yyyyMMddHHmmss")
      rdd.map{
        case(userid,count)=>(userid,timestr,count)
      }.filter(t=>t._3>30)
    }).filter(t=>t._3>30)

    //写入数据库
    finaldstream.foreachRDD(rdd=>{
      val sc = rdd.sparkContext
      val sqlContext = new SQLContext(sc)

      import  sqlContext.implicits._

      val url = ""
      val table = ""
      val prop = new Properties()
      prop.put("user","root")
      prop.put("password","123456")

      val df = rdd.toDF("userid","timestr","count").write.mode(SaveMode.Overwrite).jdbc(url,table,prop)
    })
  }
}
case class AdClickRecord(timestamp:Long,province:String,city:String,userid:Int,adid:Int)