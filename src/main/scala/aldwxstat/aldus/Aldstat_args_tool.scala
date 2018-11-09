package aldwxstat.aldus

import java.util.Calendar
import java.util.regex.Pattern
import aldwxutils.TimeUtil
import aldwxutils.TimeUtil.getDateString

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object Aldstat_args_tool {


  private var day=aldwxutils.TimeUtil.ytime()   //gcs:存储的是当前的day的时间
  private var hour = ""
  private var ak = ""  //gcs:将指定ak下的用户的数据过滤出来
  private var vak = "" //gcs:将制定下
  private var partition=0
  private var du=""
  private var filepath=""

  /**
    *
    * @param args   main方法的参数
    * @param spark  SparkSession
    * @param path   读取的文件地址
    * @param file   需要读取的日期目录下指定文件名，默认为*读取日期目录下所有文件
    * @return
    */
  def analyze_args(args: Array[String], spark: SparkSession,path:String,file:String="*"): DataFrame = {

    for (i <- 0 until args.length) {
      if ("-d".equals(args(i))) {
        if (i + 1 < args.length && chenkDate(args(i + 1))) {
            val world = args(i + 1).split("-")
            day=world(0) + world(1) + world(2)
        }
      }
      if ("-h".equals(args(i))) {
        if(i+1<args.length && checkHour(args(i+1))){
          hour=args(i+1)
        }
      }
      if ("-ak".equals(args(i))) {
        if (i + 1 < args.length && check_ak(args(i+1))) {
          ak = args(i + 1)
        }
      }
      if ("-vak".equals(args(i))) {
        if (i + 1 < args.length && check_ak(args(i+1))) {
          vak = args(i + 1)
        }
      }
      if("-np".equals(args(i))){
        if(i+1<args.length && check_np(args(i+1))){
          partition=args(i+1).toInt
        }
      }
      if("-du".equals(args(i))){
        if(i+1<args.length && (args(i+1).equals("7")|| args(i+1).equals("30"))){
          du=args(i+1)
        }
      }
    }


    if (StringUtils.isBlank(du)) {

      //读取的文件路径
      if(StringUtils.isBlank(hour)){
        filepath=s"$path/$day/$file/part-*"
      }else{
        filepath=s"$path/$day/etl-*$day$hour/part-*"
      }
      if(partition<=0){
        if (StringUtils.isNotBlank(ak) && StringUtils.isBlank(vak)) {

          val aks = split_ak(ak)
          spark.read.parquet(filepath).filter(s"ak in $aks")
        } else if (StringUtils.isBlank(ak) && StringUtils.isNotBlank(vak)) {

          val aks = split_ak(vak)
          spark.read.parquet(filepath).filter(s"ak not in $aks")
        } else {
          spark.read.parquet(filepath)
        }

      }else{
        if (StringUtils.isNotBlank(ak) && StringUtils.isBlank(vak)) {

          val aks = split_ak(ak)
          spark.read.parquet(filepath).filter(s"ak in $aks").repartition(partition)
        } else if (StringUtils.isBlank(ak) && StringUtils.isNotBlank(vak)) {

          val aks = split_ak(vak)
          spark.read.parquet(filepath).filter(s"ak not in $aks").repartition(partition)
        } else {
          spark.read.parquet(filepath).repartition(partition)
        }
      }

    } else {
      //定义一个可变数组（把循环读取到的单个文件存储到数组中）
      var data_files = ArrayBuffer[String]()

      if(day.equals(aldwxutils.TimeUtil.ytime())) {
        data_files = get_data_files(du,path,file)
      }else{
        data_files = get_day_files(du,path,file)
      }

      if(partition<=0){
        if (StringUtils.isNotBlank(ak) && StringUtils.isBlank(vak)) {
          val aks = split_ak(ak)
          spark.read.parquet(data_files:_*).filter(s"ak in $aks ")
        } else if (StringUtils.isBlank(ak) && StringUtils.isNotBlank(vak)) {
          val aks = split_ak(vak)
          spark.read.parquet(data_files:_*).filter(s"ak not in $aks")
        }else {
          spark.read.parquet(data_files:_*)
        }
      }else{
        if (StringUtils.isNotBlank(ak) && StringUtils.isBlank(vak)) {
          val aks = split_ak(ak)
          spark.read.parquet(data_files:_*).filter(s"ak in $aks ").repartition(partition)
        } else if (StringUtils.isBlank(ak) && StringUtils.isNotBlank(vak)) {
          val aks = split_ak(vak)
          spark.read.parquet(data_files:_*).filter(s"ak not in $aks").repartition(partition)
        }else {
          spark.read.parquet(data_files:_*).repartition(partition)
        }
      }

    }
  }

  /**
    * 用于重新分区
 *
    * @param df  读完数据的DataFrame
    * @param args main方法的参数
    * @return
    */
  def re_partition(df:DataFrame,args:Array[String]):DataFrame={
    for (i <- 0 until args.length) {
      if("-np".equals(args(i))){
        if(i+1<args.length && check_np(args(i+1))){
          partition=args(i+1).toInt
        }
      }
    }
    if(partition<=0){
      df
    }else{
      df.repartition(partition)
    }

  }


  //返回昨天之前7或30天数据的读取地址
  private def get_data_files(date:String,path:String,file:String):ArrayBuffer[String]={
    var T1 = ""
    var T2 = ""
    if("30".equals(date)){
      T1=TimeUtil.ytimeHive29
      T2=TimeUtil.ytimeHive
    }else{
      T1=TimeUtil.ytimeHive7
      T2=TimeUtil.ytimeHive
    }
    // 读取的文件
    val data_files = ArrayBuffer[String]()
    var dates = ""

    //开始日期
    val time1 = Aldstat_date_tool.date2Long(T1)
    //结束日期
    val time2 = Aldstat_date_tool.date2Long(T2)
    //遍历时间差
    val a = for (i <- time1 to time2 if (i % 86400 == 0)) yield i
    //遍历日期
    for (j <- a) {
      dates = TimeUtil.getDateString(j.toLong * 1000, "yyyyMMdd")

      if(StringUtils.isBlank(hour)){
        data_files += s"$path/$dates/$file/part-*"
      }else{
        data_files += s"$path/$dates/etl-*$day$hour/part-*"
      }
    }

    data_files
  }


  //返回指定日期之前7或30天数据的读取地址
  private def get_day_files(date:String,path:String,file:String):ArrayBuffer[String]={
    var T1 = ""
    var T2 = ""
    if("30".equals(date)){
      T1=get_date(day,29)
      T2=day
    } else{
      T1=get_date(day,6)
      T2=day
    }
    val time1 = TimeUtil.str2Long(T2)
    val time2 = TimeUtil.str2Long(T1)

    val time = ((time1 - time2) / 86400000).toInt

    //获得参数的第一位 时间戳
    val nowtime = TimeUtil.str2Long(T2)

    // 读取的文件
    val data_files = ArrayBuffer[String]()

    //读取 指定天的数据
    for (i <- 0 to (time)) {
      val jTime = nowtime - (i * TimeUtil.day.toLong)
      val jDay = TimeUtil.long2Str(jTime)
      //循环 读取文件,判断是否要过滤小时的数据
      if(StringUtils.isBlank(hour)){
        data_files += s"$path/$jDay/$file/part-*"
      }else{
        data_files += s"$path/$jDay/etl-*$day$hour/part-*"
      }
    }
    data_files
  }


  //获得指定日期的前几天的日期
  private def get_date(day:String,num:Int):String={
    val cal = Calendar.getInstance()
    val long:Long = TimeUtil.str2Long(day)
    cal.setTimeInMillis(long)
    cal.add(Calendar.DATE,-num)
    getDateString(cal.getTime.getTime,"yyyyMMdd")
  }


  //检查 日期格式
  private def chenkDate(date: String): Boolean = {
    //定义正则表达式yyyyMMdd
    val regex = "[2][0][0-9]{2}-[0-9]{2}-[0-9]{2}"
    //val regex = "d{4}-(0[1-9]|1[1-2])-(0[1-9]|2[0-9]|3[0-1])"
    val compile = Pattern.compile(regex)
    //匹配数据
    val matcher = compile.matcher(date)
    matcher.matches()
  }


  //检查 小时格式
  private def checkHour(date:String) :Boolean = {
    //定义正则表达式yyyyMMdd
    val regex = "[0-2][0-9]"
    val compile =Pattern.compile(regex)
    //匹配数据
    val matcher =compile.matcher(date)
    matcher.matches()
  }

  //检查 appkey的格式
  private def check_ak(ak:String) :Boolean = {

    val regex = "[a-z0-9,]{32,}"
    val compile =Pattern.compile(regex)
    //匹配数据
    val matcher =compile.matcher(ak)
    matcher.matches()
  }

  //检查 分区的格式
  private def check_np(np:String) :Boolean = {

    val regex = "[0-9]{1,}"
    val compile =Pattern.compile(regex)
    //匹配数据
    val matcher =compile.matcher(np)
    matcher.matches()
  }
  /**
    * 将传入的ak进行拼接
    */
  private def split_ak(arg: String): String = {
    val aks = arg.split(",")

    var list = new ListBuffer[String]()

    for (ak <- aks) {
      list.append("'" + ak + "'")
    }

    "(" + list.mkString(",") + ")"
  }


}
