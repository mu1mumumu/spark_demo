package com.mathartsys.yss

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

object TestUDF {
  def main(args: Array[String]): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    // 当前时间的前13个月同期
    val caleM: Calendar = Calendar.getInstance
    val historyM: Seq[String] = for (i <- 1 to 13) yield {
      caleM.setTime(new Date())
      caleM.add(Calendar.MONTH, -1 * i)
      caleM.add(Calendar.DAY_OF_MONTH, -1)
      format.format(caleM.getTime)
    }
    println(historyM)

    val caleQ: Calendar = Calendar.getInstance
    val historyQ: Seq[String] = for (i <- 1 to 20) yield {
      caleQ.setTime(new Date())
      caleQ.add(Calendar.MONTH, -3 * i)
      caleQ.add(Calendar.DAY_OF_MONTH, -1)
      format.format(caleQ.getTime)
    }
    println(historyQ)

    val caleY: Calendar = Calendar.getInstance
    val historyY: Seq[String] = for (i <- 1 to 5) yield {
      caleY.setTime(new Date())
      caleY.add(Calendar.YEAR, -1 * i)
      caleY.add(Calendar.DAY_OF_MONTH, -1)
      format.format(caleY.getTime)
    }
    println(historyY)



  }
}
