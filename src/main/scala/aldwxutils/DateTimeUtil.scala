package aldwxutils

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, Period}
import java.util.Date

/**
* <b>author:</b> wty <br>
* <b>data:</b> 2017/12/14 <br>
* <b>description:</b><br>
* <b>param:</b><br>
* <b>return:</b><br>
*/
class DateTimeUtil {


  /**
    * 获取相对于今天某天(相聚多少天)日期的指定格式
    *
    * @param day     0 今天，1 明天，-1 昨天
    * @param pattern 日期格式 正则
    * @param year    几年后
    * @param month   几月后
    * @param week    1 表示7天后的日期
    * @return 例如yyyy-MM-dd(指定规则pattern)
    */
  def getUdfDate(day: Int, pattern: String, year: Int = 0, month: Int = 0, week: Int = 0): String = {
    val dtf: DateTimeFormatter = DateTimeFormatter.ofPattern(pattern)
    LocalDate.now().plusYears(year).plusMonths(month).plusWeeks(week).plusDays(day).format(dtf)
  }

  def long2string(timeStamp: Long, pattern: String): String = {
    val sdf = new SimpleDateFormat(pattern)
    sdf.format(new Date(timeStamp))
  }

  def string2long(date: String, pattern: String): Long = {
    val sdf = new SimpleDateFormat(pattern)
    sdf.parse(date).getTime
  }

  //def intervalDays(first:LocalDate,second:LocalDate,day:Int)


  /**
    * 获取指定日期day，往前numDays天的hdfs目录path所组成的数组
    *
    * @param day     指定日期
    * @param numDays 要获取的天数
    * @return 包含hdfs多个路径的数组
    */
  def getUdfDaysDirs(path: String, day: String, numDays: Int): Array[String] = {
    val dtf: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    //要读取的日志目录数组
    var dirs = Array[String]()
    val ymd = day.split("-")
    val year = ymd(0)
    val month = ymd(1)
    val dayOfMonth = ymd(2)

    val date = LocalDate.of(year.toInt, month.toInt, dayOfMonth.toInt)

    for (i <- -numDays + 1 to 0) {
      val logDir = path + "/" + date.plusDays(i).format(dtf)
      println(s"读取$logDir/*/*")
      //if (new HDFS().isDirectory(logDir))
      dirs = dirs.+:(logDir + "/*/*")
    }
    dirs
  }


  /**
  * <b>author:</b> gcs <br>
  * <b>data:</b> 2017120313 <br>
  * <b>description:</b><br>
    *   弹性mr日志目录，获取指定日期往前7天或30天的数据路径的数组 <br>
    *     目录的格式是： cosn://aldwxlogbackup/log_parquet/zzcg-etl-VM-0-224-ubuntu2017120313/ <br>
    *                 cosn://aldwxlogbackup/log_parquet/2017120313/ <br>
  * <b>param:</b><br>
    *   day:String ;初始的天数 <br>
    *     numDays:Int ;在初始的天数的基础之上获得numDays天的数据 <br>
  * <b>return:</b><br>
    *   Array[String];天数路径的数组 <br>
  */
  def getTencentDirs(day:String,numDays:Int): Array[String] ={
    val dtf: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd") //gcs:定义数据的格式

    val ymd = day.split("-")
    val year = ymd(0) //gcs:获得日期中的year
    val month = ymd(1) //gcs:获得日期中的month
    val dayOfMonth = ymd(2) //gcs:获得那个月中的day天数

    val date = LocalDate.of(year.toInt, month.toInt, dayOfMonth.toInt) //gcs:把year和month以及day整理在一起

    //--------------gcs:做了更改 1.0出更改 "cosn://aldwxlogbackup/log_parquet/*"
//    val hdfsUrl="cosn://aldwxlogbackup/log_parquet/*"  //gcs:这个目录是7天的文件的数据所在的目录
    var dirs = Array[String]()
    val hdfsUrl = "hdfs://10.0.100.17:4007/ald_log_parquet/"

    for (i <- -numDays + 1 to 0) { //gcs:假如现在 day=2018-05-10 numDays 等于1，那么logDir只会获得 cosn://aldwxlogbackup/log_parquet/*20180510*/一天的数据
      //gcs:假如现在 day = 2018-05-10 numDay等于2，那么会获得cosn.../*20180509*/和cosn.../*20180510*/的数据，即从20180510倒数2天

      //---------------gcs:2.0处更改 s"$hdfsUrl${date.plusDays(i).format(dtf)}*"
//      val logDir =s"$hdfsUrl${date.plusDays(i).format(dtf)}*"   //gcs:把刚才的hdfsUrl这个目录增添天数的子目录

      val logDir =s"$hdfsUrl${date.plusDays(i).format(dtf)}"
      println(s"读取$logDir/*")
      //if (new HDFS().isDirectory(logDir))
      dirs = dirs.+:(logDir + "/*")
    }
    dirs
  }


  /**
  * <b>author:</b> wty <br>
  * <b>data:</b> 18-5-7 <br>
  * <b>description:</b><br>
    *   获取erm指定天数的数据集合，可以和上面获取数据的方法整合成一个，后续再改吧 <br>
  * <b>param:</b><br>
    *   day:String ;从指定的日期开始算，默认是昨天 <br>
    *     daysArr:Array[Int] ;数组里面存要获取的天数，-1表示1天前。比如daysArr={3}，就相当于获取第 day-3 天的那一天的数据 <br>
  * <b>return:</b><br>
    *   Array[String] ;返回要获取的所有天数的emr数据路径的数组 <br>
  */
  def getSpecifyTencentDirs(day:String,daysArr:Array[Int]): Array[String] ={
    val dtf: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")

    val ymd = day.split("-")
    val year = ymd(0)
    val month = ymd(1)
    val dayOfMonth = ymd(2)

    val date = LocalDate.of(year.toInt, month.toInt, dayOfMonth.toInt) //gcs:将year,month,dayOfMonth 合成规格的 year-month-dayOfMonth 的格式
    val hdfsUrl="cosn://aldwxlogbackup/log_parquet/*"
    var dirs = Array[String]()

    //gcs:使用当前的day-i 天，将第"day-i"天的数据提取出来
    for (i <- daysArr) {
      val logDir =s"$hdfsUrl${date.plusDays(-i).format(dtf)}*"
      println(s"读取$logDir/*")
      //if (new HDFS().isDirectory(logDir))
      dirs = dirs.+:(logDir + "/*") //gcs:logDir/* 读取logDir目录中的全部的日期内容
    }
    dirs //gcs:返回要读取的数组的数组
  }

}

object DateTimeUtil {
  def main(args: Array[String]): Unit = {
    val str = new DateTimeUtil().getUdfDate(0, "yyyyMMdd", 0, 1, 1)
    println(str)
    println("2018-04-10">"2018-04-01")
    import java.time.format.DateTimeFormatter
    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    val a = LocalDate.parse("20180410",formatter)
    val b = LocalDate.now()
    println(Period.between(a,b).getMonths)
    val day="20180416"
    val date=LocalDate.of(day.substring(0,4).toInt,day.substring(4,6).toInt,day.substring(6,8).toInt)
    val hdfsUrl="cosn://aldwxlogbackup/log_parquet/*"
    var dirs = Array[String]()
    for (i <- -7 + 1 to 0) {
      val logDir =s"$hdfsUrl${date.plusDays(i).format(formatter)}*"
      println(s"读取$logDir/*")
      //if (new HDFS().isDirectory(logDir)) dirs = dirs.+:(logDir + "/*/*")
    }
  }
}