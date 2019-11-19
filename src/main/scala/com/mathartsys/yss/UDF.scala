package com.mathartsys.yss

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}



object UDF {

  val format = new SimpleDateFormat("yyyy-MM-dd")

  // 当前时间的前13个月同期
  val caleM: Calendar = Calendar.getInstance
  val historyM: Seq[String] = for (i <- 1 to 13) yield {
    caleM.setTime(new Date())
    caleM.add(Calendar.MONTH, -1 * i)
    caleM.add(Calendar.DAY_OF_MONTH, -1)
    format.format(caleM.getTime)
  }

  // 当前时间的前20季（5年）同期
  val caleQ: Calendar = Calendar.getInstance
  val historyQ: Seq[String] = for (i <- 1 to 20) yield {
    caleQ.setTime(new Date())
    caleQ.add(Calendar.MONTH, -3 * i)
    caleQ.add(Calendar.DAY_OF_MONTH, -1)
    format.format(caleQ.getTime)
  }

  // 当前时间的前5年同期
  val caleY: Calendar = Calendar.getInstance
  val historyY: Seq[String] = for (i <- 1 to 5) yield {
    caleY.setTime(new Date())
    caleY.add(Calendar.YEAR, -1 * i)
    caleY.add(Calendar.DAY_OF_MONTH, -1)
    format.format(caleY.getTime)
  }

  /**
    * @description 判断传入的时间是否为近13个月同期
    * @param before
    * @return Boolean
    */
  def thirteenMonth(before: String): Boolean = {
    if (historyM.contains(before)) true else false
  }

  /**
    * @description 判断传入的时间是否为近20个季同期
    * @param before
    * @return Boolean
    */
  def twentyQuarter(before: String): Boolean = if (historyQ.contains(before)) true else false

  def fiveYear(before: String): Boolean = {
    if (historyY.contains(before)) true else false
  }


  /**
    * @description 判断 dateB 是否为 dateA 的前三年同期
    * @param dateA
    * @param dateB
    * @return Boolean
    */
  def threeYear(dateA: String, dateB: String): Boolean = {
    if (dateA != null && dateB != null) {
      val caleTY = Calendar.getInstance()
      caleTY.setTime(format.parse(dateA))
      for (i <- 1 to 3) {
        caleTY.add(Calendar.YEAR, -1)
        if (dateB.equals(format.format(caleTY.getTime))) return true
      }
    }
    false
  }

  /**
    * @description 判断 dateB 是否为 dateA 的上月同期、去年同期
    * @param dateA
    * @param dateB
    * @return Boolean
    */
  def monthYear(dateA: String, dateB: String): Boolean = {
    if (dateA != null && dateB != null) {
      val caleTY = Calendar.getInstance()
      caleTY.setTime(format.parse(dateA))
      caleTY.add(Calendar.YEAR, -1)
      if (dateB.equals(format.format(caleTY.getTime))) return true
      caleTY.setTime(format.parse(dateA))
      caleTY.add(Calendar.MONTH, -1)
      if (dateB.equals(format.format(caleTY.getTime))) return true
    }
    false
  }

  /**
    * @description 计算 date 的 n 月前的同期
    * @param date
    * @param n
    * @return String
    */
  def getOnMonth(date: Object, n: Int): String = {
    try {
      val cale: Calendar = Calendar.getInstance
      val dt = date match {
        case date1: Date => date1
        case date2:String => format.parse(date2)
      }
      cale.setTime(dt)
      cale.add(Calendar.MONTH, 1)
      cale.set(Calendar.DAY_OF_MONTH, 0)
      if (date.equals(format.format(cale.getTime))) {
        cale.setTime(dt)
        cale.add(Calendar.MONTH, n + 1)
        cale.set(Calendar.DAY_OF_MONTH, 0)
        format.format(cale.getTime)
      } else {
        cale.setTime(dt)
        cale.add(Calendar.MONTH, n)
        format.format(cale.getTime)
      }
    } catch {
      case e:Exception =>
        null
    }
  }
}
