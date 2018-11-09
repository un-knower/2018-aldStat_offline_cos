package aldwxstat.aldus

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}

import org.apache.commons.lang.time.DateFormatUtils

/**
  * Created by dell on 2017/7/10.
  *
  * 时间工具类
  */
object Aldstat_date_tool {
  def main(args: Array[String]): Unit = {
     val a=1501603200000L
    println(getDateString(a,"yyyyMMdd"))
  }


  //日期转时间戳
  def date2Long(str: String): Long = {
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date = sdf.parse(str)
    val ts = date.getTime
    ts / 1000
  }


  def Date2FormatDate(date: Date, pattern: String): String = {
    return DateFormatUtils.format(date, pattern, Locale.ENGLISH)
  }


  //获取昨天的日期
  def ytime(): String = {
    //创建calendar对象
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    val format = new SimpleDateFormat("yyyyMMdd")
    val ydate = cal.getTime
    val newydate = format.format(ydate)
    newydate
  }


  //获取当天的日期
  def getNowDate(): String = {
    var now: Date = new Date()
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    var today = dateFormat.format(now)
    today
  }


  //获取本月第一天
  def getNowMonthStart(): String = {
    var NowMonthStart: String = ""
    var cal: Calendar = Calendar.getInstance();
    var df: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd");
    cal.set(Calendar.DATE, 1)
    NowMonthStart = df.format(cal.getTime()) //本月第一天
    NowMonthStart
  }

  //获取本月
  def getNowMonth(): String = {
    var NowMonth: String = ""
    var cal: Calendar = Calendar.getInstance();
    var df: SimpleDateFormat = new SimpleDateFormat("yyyyMM");
    //cal.set(Calendar.DATE, 1)
    NowMonth = df.format(cal.getTime()) //本月第一天
    NowMonth
  }


  //判断本月有多少天
  def getNowDays(): String = {

    //本月的统计
    //本月开始日期
    val nowmonthstart = getNowMonthStart()
    //前一天时间
    val nowDay: String = getNowDate()

    val cal: Calendar = Calendar.getInstance()
    cal.setTime(new Date())
    var df: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd");

    //判断这个月有多少天
    val a = df.parse(nowmonthstart)
    val aa = df.parse(nowDay)
    val aTime = (a.getTime() / 1000).toInt
    val bTime = (aa.getTime / 1000).toInt
    val abTime = bTime - aTime
    val date: String = (abTime / 86400).toString
    date
  }

  //上个月的第一天
  def getOldMonthStart(): String = {
    var OldMonthStart: String = ""
    //var b:Int=0
    val cal: Calendar = Calendar.getInstance()
    cal.setTime(new Date())
    var df: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd");
    cal.add(Calendar.MONTH, -1)
    // cal.add(Calendar.MONDAY,1)
    OldMonthStart = df.format(cal.getTime)
    var b = Integer.parseInt(getNowDate()) - Integer.parseInt(getNowMonthStart())
    var lastMonth = Integer.parseInt(OldMonthStart) - b

    lastMonth.toString


  }

  //上个月的月份
  def getOldMonth(): String = {
    var OldMonth: String = ""
    //var b:Int=0
    val cal: Calendar = Calendar.getInstance()
    cal.setTime(new Date())
    var df: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd");
    cal.add(Calendar.MONTH, -1)
    // cal.add(Calendar.MONDAY,1)
    OldMonth = df.format(cal.getTime)
    val date = OldMonth.substring(0, 6)
    date

  }

  //上个月有多少天
  def getlastMonthDays(): String = {
    val cal: Calendar = Calendar.getInstance()
    cal.setTime(new Date())
    var df: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd");
    val a = df.parse(getOldMonthStart())
    val aa = df.parse(getNowMonthStart())
    val aTime = (a.getTime() / 1000).toInt
    val bTime = (aa.getTime / 1000).toInt
    val abTime = bTime - aTime

    val date: String = (abTime / 86400).toString
    date
  }
  //近七天前的首天日期（计算）
  def getWeekStart(): String = {
    //今天的日期
    val today = getNowDate()
    //println(today)
    val format = new SimpleDateFormat("yyyyMMdd")
    val today1 = format.parse(today)
    //七天以前的时间
    val nowtime3 = (today1.getTime() / 1000).toInt - 86400 * 7
    //println(nowtime3)
    val times2 = format.format(new Date(nowtime3.toLong * 1000))
    times2
  }


  //时间戳转日期
  def getDateString(millis: Long, pattern: String): String = {
    return DateFormatUtils.format(millis, pattern, Locale.ENGLISH)
  }


}
