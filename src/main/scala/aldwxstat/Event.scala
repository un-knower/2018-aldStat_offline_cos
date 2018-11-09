//==========================================================
/*gcs:
*事件分析-事件列表
*转化漏斗
*/

package aldwxstat

import aldwxconfig.ConfigurationUtil
import aldwxutils.{ArgsTool, JdbcUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer



/**
  * Created by gaoxiang on 2018/1/11.
  */
object Event {

  /**
  * <b>author:</b> gcs <br>
  * <b>data:</b> 18-5-15 <br>
  * <b>description:</b><br>
    *   转化漏斗、事件分析的今天、昨天、7天、30天的数据分析 <br>
  * <b>param:</b><br>
    *   args: Array[String] ;程序在运行的时候的参数。
    *   -d xxxx  指定分析 xxxx只一天的数据。xxxx的数据格式是"2017-05-03"
    *   -d xxxx -du y 指定从xxxx那天开始，分析前 y 天的数据。如，-d 2018-05-15 -du 7 指的是从2018-05-15那天开始分析7天的数据
    *   -ak 指定分析哪一个ak
  * <b>return:</b><br>
  */
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)  //gcs:设置log日志的级别


    val ss = SparkSession.builder()
      .appName(this.getClass.getName)
      .getOrCreate()


    //获取昨天时间
    val yesterday = aldwxutils.TimeUtil.processArgs(args)  //gcs:如果程序中有 -d 2018-08-05 这样的参数的字样，yesterday 就是20180805

    //==========================================================1
    //gcs:主程序
    allEventStatistics(ss, yesterday, args)

    ss.close()
  }


  /**
  * <b>author:</b> gcs <br>
  * <b>data:</b> 18-5-8 <br>
  * <b>description:</b><br>
    *   spark: SparkSession ; <br>
    *    yesterday: String ; yesterday 这个时间的作用是什么呢<br>
  * <b>param:</b><br>
  * <b>return:</b><br>
  */
  def allEventStatistics(spark: SparkSession, yesterday: String, args: Array[String]) = {

    //==========================================================2
    //gcs:分析各种参数 -d -du 等参数
    ArgsTool.analysisArgs(args)

    //gcs:获得du
    val du = ArgsTool.du
    //    val df = Aldstat_args_tool.analyze_args(args, spark, DBConf.hdfsUrl).filter("ev='event' and tp!='null'")
//    val df = ArgsTool.getLogs(args, spark, ConfigurationUtil.getProperty("tongji.parquet")).filter("ev='event' and tp!='null'")

//    val df =ArgsTool.getTencentDailyDataFrame(spark, ConfigurationUtil.getProperty("tencent.parquet")).filter("ev='event' and tp!='null'")

    val df = ArgsTool.getTencentSevenOrThirty(spark,du) //gcs:按照du，读取从day偏移的位置读取du天数的数据

    df.createTempView("daily_event") //gcs:创建一个临时的视图

    //==========================================================3
    /*gcs:
    *创建一个新的类 allEventCount
    */
    new allEventCount(spark, yesterday, df, du).eventCount()
  }
}



/**
* <b>author:</b>gx <br>
* <b>data:</b> 2018/1/11 <br>
* <b>description:</b><br>
  *   spark: SparkSession ;传进来一个sparkSession对象，这个sparkSession对象是用来操作spark程序的 <br>
  *     yesterday: String ;yesterday 是一个时间参数。<br>
  *       df: Dataset[Row] ;从ArgsTool.day 开始读取du参数指定的天数的数据 <br>
* <b>param:</b><br>
* <b>return:</b><br>
*/
class allEventCount(spark: SparkSession, yesterday: String, df: Dataset[Row], du:String) extends Serializable {



  def eventCount(): Unit = {
    //读取mysql 中 的数据（ald_event）
    //gcs:从ald_event 这个表中读取到 所有的event事件的编号。用来在后面与daily_event和ald_event做join操作
    /*gcs:
    ald_event 表当中存储着微信小程序的app_key 和该微信小程序中的event_key 的对应关系。什么是事件呢。我的理解，“事件”实际上就是微信小程序的一个子的功能模块
    *mysql> show columns from ald_event;
+----------------+--------------+------+-----+-------------------+----------------+
| Field          | Type         | Null | Key | Default           | Extra          |
+----------------+--------------+------+-----+-------------------+----------------+
| id             | int(11)      | NO   | PRI | NULL              | auto_increment |
| app_key        | varchar(255) | NO   | MUL | NULL              |                |
| ev_id          | varchar(255) | NO   |     | NULL              |                |
| event_key      | varchar(255) | NO   |     | NULL              |                |
| ev_name        | varchar(255) | NO   |     | NULL              |                |
| ev_status      | tinyint(1)   | NO   |     | 1                 |                |
| ev_update_time | timestamp    | NO   |     | CURRENT_TIMESTAMP |                |
+----------------+--------------+------+-----+-------------------+----------------+

mysql> select * from ald_event limit 10;
+----------+----------------------------------+-------------------+----------------------------------+-------------------------------+-----------+---------------------+
| id       | app_key                          | ev_id             | event_key                        | ev_name                       | ev_status | ev_update_time      |
+----------+----------------------------------+-------------------+----------------------------------+-------------------------------+-----------+---------------------+
| 35030752 | 61b129ce5e318171f3cd9eec2b415c8d | ald_share_status  | c8fea4823abc0a86d36fe5f531a51f80 | ald_share_status              |         1 | 2018-05-13 17:09:18 |
| 35030753 | 3d3db58fee367efd6af41c621c80468e | ald_share_click   | caebc4c737f785ff7bcbd07864660362 | ald_share_click               |         1 | 2018-05-15 14:02:10 |
| 35030754 | 0fe03d20fc1e66599984ba2165852c6e | ald_error_message | 5db1c6b285d7f4590a58e20dd27fbfcf | ald_error_message             |         1 | 2018-03-22 14:29:15 |
| 35030755 | 1d154aeefbb27bf5301328f76c8826c7 | ald_reachbottom   | f26f879f30fd8d9c2b7752394e41ffcb | 页面触底(阿拉丁默认)          |         1 | 2018-05-15 14:02:22 |
| 35030756 | 54117664fad164a634c57571ba7c5aa7 | ald_share_click   | 20c258fac9c713d353784313c35f9fff | ald_share_click               |         1 | 2018-05-15 14:03:01 |



id       是这个ald_event表用来标识用户的记录的，是一个自增值
app_key     微信小程序唯一的标识的app_key
ev_id    event的id标识。即原始上报类型的json数据中的tp字段
event_key  微信小程序创建的唯一可以标识一个event_key 。event_keu是ak+tp字段的MD5的加密
ev_name   这创建的事件的名字
ev_status  当前的这个event所处于的状态。如果为 1，表示这个event现在处于被使用的状态。如果为 0，表示这个event现在处于被报废的状态
ev_update_time   这个event被更新的时间。实际上这个值是公司的jar包被
    */
    val event_df = JdbcUtil.readFromMysql(spark, "(select app_key,ev_id,event_key,ev_name from ald_event) as event_df") //gcs:将数据库中的数据读取出来

    //事件人数和打开次数。
    // 从daily_event当读取数据，之后再做去重的操作。这里就是计算at的count的值。
    //gcs:at 字段:acess accept token 会话
    //gcs:trigger_user_count 是触发用户
    //==========================================================2-1
    /*
    *gcs:
    *在事件分析的“事件列表”的维度下的 触发用户数 是由distinct(uu) 计算出来的；“触发次数”是由cout(at)计算出来的。人均次数是由 cast(count(at)/count(distinct(uu)) as float 计算出来的
    * 在另外的EventParas这个文件中，事件分析的"参数明细"的参数的“消息数”指标是由 “ak+tp+事件参数的name+事件参数的value”计算出来的。
    */
    val dataFrame = spark.sql("select ak,tp,count(distinct(uu)) count_uu,count(at) count_at,cast(count(at)/count(distinct(uu)) as float) trigger_user_count,now() time from daily_event group by ak,tp").distinct()



    //==========================================================4
    /*gcs:
    *event_df 和 dataFrame 进行join的操作。之后进行select的操作
    * 将dataFrame和event_df进行Join的操作，当dataFrame中的一行记录中的 ak和tp分别等于event_df中的app_key 和ev_id 的时候，此时将这两行数据合成一行数据
    */
      //把读取数据库的数据和读取日志的数据进行join 条件是日志的tp = 数据库的ev_id

    val result = dataFrame.join(event_df, dataFrame("ak") === event_df("app_key") &&
      dataFrame("tp") === event_df("ev_id"))
      .select(
        event_df("app_key"),
        event_df("ev_id"), //gcs:ev_id 是parquet当中的tp字段，即事件的类型
        event_df("event_key"), //gcs：当前的这个事件的编码。event_key =ak+tp的MD5的加密
        dataFrame("count_uu"),
        dataFrame("count_at"),
        dataFrame("time"),
        dataFrame("trigger_user_count")  //gcs: cast(count(at)/count(distinct(uu)) as float 。at/uu
      )

    //==========================================================5
    /*gcs:
    *将result数据插入到MySql数据库当中
    */
    new eventInsert2Mysql(result, yesterday, du).insertmysql()
  }

}


/**
* <b>author:</b> gcs <br>
* <b>data:</b> 18-5-8 <br>
* <b>description:</b><br>
  *   把event数据插入进MySql数据库 <br>
* <b>param:</b><br>
  *   result: DataFrame ;要将哪些数据插入到DataFrame当中 <br>
  *     yesterday: String ;???这个参数的作用是什么 <br>
  *       du:String ; du参数代表要处理哪些数据 <br>
* <b>return:</b><br>
*/
class eventInsert2Mysql(result: DataFrame, yesterday: String, du:String) extends Serializable {


  /**
  * <b>author:</b> gcs <br>
  * <b>data:</b> 18-5-8 <br>
  * <b>description:</b><br>
  * <b>param:</b><br>
    *   null <br>
  * <b>return:</b><br>
    *   null <br>
  */
  def insertmysql(): Unit = {

    //gcs:按照每一个partition进行操作
    result.foreachPartition((row: Iterator[Row]) => {

      val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
      var sqlText = ""

      row.foreach(r => {
        //conn.setAutoCommit(false)

        val app_key = r.get(0)
        val day = yesterday
        val ev_id = r.get(1)
        val event_key = r.get(2)
        val trigger_user_count = r.get(3) //触发人数 count(uu)
        val trigger_count = r.get(4) //触发次数  count(at)
        val update_at = r.get(5)
        val avg_trigger_count = r.get(6) //人均触发数


        //==========================================================6
        //gcs:根据du的值将数据插入到数据库当中
        /*gcs:
        *du可以取7或者30.当du取7或者30的时候，这时候就会将统计到的数据插入到 aldstat_7days_event 或者 aldstat_30days_event 当中
        */
        if (du == "") {
          sqlText = "insert into aldstat_daily_event (app_key,day,ev_id,event_key,trigger_user_count,trigger_count,avg_trigger_count,update_at)" + s"values (?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE trigger_user_count=?, trigger_count=?,avg_trigger_count=?,update_at=?"
          params.+=(Array[Any](app_key, day, ev_id, event_key, trigger_user_count, trigger_count, avg_trigger_count, update_at, trigger_user_count, trigger_count, avg_trigger_count, update_at))
        } else {
          sqlText = s"insert into aldstat_${du}days_event (app_key,day,ev_id,event_key,trigger_user_count,trigger_count,avg_trigger_count,update_at)" + s"values (?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE trigger_user_count=?, trigger_count=?,avg_trigger_count=?,update_at=?"
          params.+=(Array[Any](app_key, day, ev_id, event_key, trigger_user_count, trigger_count, avg_trigger_count, update_at, trigger_user_count, trigger_count, avg_trigger_count, update_at))
        }

        /* for (i <- 0 until args.length) {
          if ("-d".equals(args(i))) {
            sqlText = "insert into aldstat_daily_event (app_key,day,ev_id,event_key,trigger_user_count,trigger_count,avg_trigger_count,update_at)" + s"values (?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE trigger_user_count=?, trigger_count=?,avg_trigger_count=?,update_at=?"
            params.+=(Array[Any](app_key, day, ev_id, event_key, trigger_user_count, trigger_count, avg_trigger_count, update_at, trigger_user_count, trigger_count, avg_trigger_count, update_at))
          } else if ("-du".equals(args(i))) {
            if (i + 1 < args.length && args(i + 1).equals("7")) {
              sqlText = "insert into aldstat_7days_event (app_key,day,ev_id,event_key,trigger_user_count,trigger_count,avg_trigger_count,update_at)" + s"values (?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE trigger_user_count=?, trigger_count=?,avg_trigger_count=?,update_at=?"
              params.+=(Array[Any](app_key, day, ev_id, event_key, trigger_user_count, trigger_count, avg_trigger_count, update_at, trigger_user_count, trigger_count, avg_trigger_count, update_at))
            } else if (i + 1 < args.length && args(i + 1).equals("30")) {
              sqlText = "insert into aldstat_30days_event (app_key,day,ev_id,event_key,trigger_user_count,trigger_count,avg_trigger_count,update_at)" + s"values (?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE trigger_user_count=?, trigger_count=?,avg_trigger_count=?,update_at=?"
              params.+=(Array[Any](app_key, day, ev_id, event_key, trigger_user_count, trigger_count, avg_trigger_count, update_at, trigger_user_count, trigger_count, avg_trigger_count, update_at))
            }
          }
        }*/
      })

      //gcs:按照批次将数据插入到数据库
      JdbcUtil.doBatch(sqlText, params) //批量入库
    })
  }
}
