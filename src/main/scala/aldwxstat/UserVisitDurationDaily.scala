//==========================================================
/*gcs:
*忠诚度-访问时长
*/

package aldwxstat

import aldwxconfig.ConfigurationUtil
import aldwxutils.{ArgsTool, JdbcUtil, TimeUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by gaoxiang on 2017/8/8.
  * 忠诚度访问时长
  */
object UserVisitDurationDaily {
  // 设置日志级别为WARN
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {

    val yesterday = TimeUtil.processArgs(args)  //gcs:获得"-d 2018-05-10"中的yesterday的参数，yesterday=20180510
    //获取昨天时间
    val ss = SparkSession.builder().appName(this.getClass.getName)
      .getOrCreate()  //gcs:获得一个ss对象
    //读取数据

    val datafream = ArgsTool.getLogs(args, ss, ConfigurationUtil.getProperty("tongji.parquet")) //gcs:根据
    //创建临时视图


    //==========================================================n2-2
    /*
    *gcs:
    *将我们的日志读出来之后，创建一个新的 t_user_loyal 模块
    */
    datafream.createTempView("t_user_loyal") //gcs:创建一个临时的视图

    //这是为了补0根据访问时长的维度来创建
    //==========================================================n2-1
    /*
    *gcs:
    *根据访问时长，来设定访问的间隔。从我们的忠诚度-访问时长，中就可以判断出来，我们的访问时长级别一共有8个级别
    */
    val df = ss.createDataFrame(List(("1", 0), ("2", 0), ("3", 0), ("4", 0), ("5", 0), ("6", 0), ("7", 0), ("8", 0)))


    //==========================================================n2-3
    /*
    *gcs:
    *设定page_depth的深度。这是什么问题呢？？？？
    */
    val lpDF = df.withColumnRenamed("_1", "page_depth").withColumnRenamed("_2", "ak") //指定字段
    lpDF.createTempView("df_a")


    //==========================================================n2-4
    /*
    *gcs:
    *从刚刚提取出来的数据里面将 distinct 的ak，提取出来
    */
    ss.sql("select distinct ak  from t_user_loyal").createTempView("df_b")


    //==========================================================n2-4
    /*
    *gcs:
    *将提取了ak的df_a和存储了8个page_depth的df_b进行笛卡尔积(cross join)的操作
    * 最终形成的结果是，每一个ak都有了8种page_depth
    */
    val view = ss.sql("select df_b.ak app_key, df_a.page_depth from df_a CROSS JOIN df_b  ").createTempView("df_c")



    //==========================================================n2-5
    /*
    *gcs:
    *"at"  : "启动时上产生的access_token",
    * "st"  : "启动时间",
    * cast(max(st)/1000 as int)-cast(min(st)/1000  用户的最大的启动一个小程序的page的时间减去用户的最小的启动page的时间，就是这个用户的访问时长
    * 在计算出了访问时长之后，要根据这个用户的访问时长的大小，要把这个用户分为(0,2)->1;(3,5)->2;(6,10)->3;(11,20)->4....等8个级别
    */
    //不同访问时长人数
    //计算 用户访问时长 用最大值减去最小值
    ss.sql("SELECT ak,uu,at,cast(max(st)/1000 as int)-cast(min(st)/1000 as int) p_dr " +
      "FROM t_user_loyal WHERE ev='page' GROUP BY ak,uu,at").createTempView("a")
    val t = ss.sql("SELECT ak app_key,uu,at, (case when (p_dr >=0 and p_dr<=2)  then '1' " +
      "when (p_dr >=3 and p_dr<=5)   then '2' " +
      "when (p_dr >=6 and p_dr<=10)  then '3' " +
      "when (p_dr >=11 and p_dr<=20)  then '4' " +
      "when (p_dr >=21 and p_dr<=30)  then '5' " +
      "when (p_dr >=31 and p_dr<=50)  then '6' " +
      "when (p_dr >=51 and p_dr<=100)  then '7' " +
      "else '8' end) dr_pepth FROM a  GROUP BY ak,uu,at,p_dr").createTempView("b")


    //==========================================================n2-6
    /*
    *gcs:
    *计算同一个ak下的打开次数。根据at字段进行计算
    */
    //ak uu分组查询打开次数
    ss.sql("SELECT ak app_key,uu,at,count(DISTINCT at) as pps   FROM" +
      " t_user_loyal WHERE ev='app' GROUP BY ak,uu,at").createTempView("c")


    //==========================================================n2-7
    /*
    *gcs:
    *把一个ak下每一个访问时长的级别的dr_pepth与访问人数绑定起来
    */
    //连接访问时长标记表表
    ss.sql("SELECT b.app_key,b.uu,b.dr_pepth,c.pps FROM b LEFT JOIN c ON b.app_key = c.app_key AND" +
      " b.uu = c.uu AND b.at=c.at ").createTempView("d")


    //==========================================================n2-8
    /*
    *gcs:
    *计算同一个ak下的访问人数uu
    */
    //访问人数 打开次数
    ss.sql("SELECT app_key,dr_pepth,count(distinct uu) as num_of_user, " +
      "  sum(pps) as vists_times " +
      " from d group by app_key,dr_pepth  ").createTempView("e")


    //==========================================================n2-9
    /*
    *gcs:
    *分别计算同一个ak下的所有的“访问次数”和“打开次数”的总和
    * sum(num_of_user) "访问人数"的总和，即 打开总人数
    * sum(vists_times) "打开次数"的总和，即 打开页面总次数
    */
    //
    ss.sql("select app_key, sum (num_of_user) as users,sum(vists_times) as times from e group by app_key ").createTempView("f")


    //
    val t_f = ss.sql("SELECT e.app_key,e.dr_pepth,e.num_of_user, " +
      " (cast(num_of_user as double) / cast(f.users as double)) AS percent ," +
      " e.vists_times, " +
      " (cast(vists_times as double) / cast(f.times as double)) AS t_percent " +
      " from e left join f on e.app_key=f.app_key ").na.fill(0)
    t_f.createTempView("ff")


    val result = ss.sql("select df_c.app_key," + yesterday + " day,df_c.page_depth,ff.num_of_user,ff.percent,ff.vists_times" +
      " ,ff.t_percent, NOW() as update_at from df_c  left join ff on (df_c.page_depth=ff.dr_pepth and df_c.app_key=ff.app_key) where df_c.app_key!='null'  ").na.fill(0)

    try {
      result.foreachPartition((rows: Iterator[Row]) => {

        val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
        val sqlText = s"insert into aldstat_visit_duration (app_key,day,visit_duration,visitor_count, visitor_ratio," +
          s" open_count,open_ratio,update_at)" +
          s"values (?,?,?,?,?,?,?,?) ON " +
          s"DUPLICATE KEY UPDATE visitor_count=?, visitor_ratio=?, open_count=?,open_ratio=?"
        rows.foreach(r => {
          val app_key = r.get(0) //gcs:app_key; df_c.app_key
          val day = r.get(1) //gcs:day; day
          val visit_duration = r.get(2) //gcs:visit_duration;df_c.page_depth 访问深度。这个访问深度可以取值"0~8"个等级
          val visitor_count = r.get(3) //gcs:dff.num_of_user 这个ak下的所有的“访问人数”
          val visitor_ratio = r.get(4)  //gcs: ff.percent 当前的这个“访问深度”下的用户总和占这个app下的总的访问人数的比例
          val open_count = r.get(5)  //gcs:ff.vists_times 这个ak下的所有的“打开次数”总和。即各个访问深度下的“打开次数”的总和
          val open_ratio = r.get(6) //gcs：当前的这个访问深度下的打开次数占这个ak下的所有的打开此书的总和。 当前访问深度下的打开次数/这个ak下的打开次数的总和
          val update_at = r.get(7)  //gcs:系统当前的时间 NOW()
          params.+=(Array[Any](app_key, day, visit_duration, visitor_count, visitor_ratio, open_count, open_ratio, update_at, visitor_count, visitor_ratio, open_count, open_ratio))
        })
        JdbcUtil.doBatch(sqlText, params)
      })
    } catch {
      case e: Exception => e.printStackTrace()
      //        connection.close()
    }
    //    关闭资源
    ss.close()

  }
}
