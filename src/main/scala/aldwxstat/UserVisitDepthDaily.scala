//==========================================================
/*gcs:
*忠诚度-访问深度
*/

package aldwxstat

import aldwxconfig.ConfigurationUtil
import aldwxutils.{ArgsTool, JdbcUtil, TimeUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer


object UserVisitDepthDaily {

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org.apache.spark").setLevel(Level.WARN) //gcs:设定日志的级别
    //获取昨天时间
    val yesterday = TimeUtil.processArgs(args)  //gcs:yesterday等于"-d 2017-05-10" 中的20170510

    val ss = SparkSession.builder().appName(this.getClass.getName)
      .getOrCreate() //gcs:创建session对象


    val datafream = ArgsTool.getLogs(args, ss, ConfigurationUtil.getProperty("tongji.parquet")) //gcs:从 hdfs://10.0.0.212:9000/ald_log_parquet 当中，读取du 天的数据



    datafream.createTempView("t_user_loyal") //gcs:创建临时视图 t_user_loyal


    //给某些ak下某些维度没有的补0
    //gcs:这里是什么意思呢??? 为什么要创建这些List数组类型呢。
    //gcs:这个是我们的忠诚度中的访问深度。我们的忠诚度一共有6页。每次访问前4页就会被设置为对应的页数。访问的pp字段为为5~10，此时就会被赋值为5。
    // pp的值如果>10，此时就会就会被赋值为6，即第6级别上的访问的深度。在写实时的代码的时候，我是可以在将line2Bean 这个函数中根据pp的值，为其重新地赋值的。
    val df = ss.createDataFrame(List(("1", 0), ("2", 0), ("3", 0), ("4", 0), ("5", 0), ("6", 0))) //gcs:使用Scala的数据创建一个DataFrame


    //gcs:对视图中的clown重新进行命名
    /*
    *gcs:
    *将表结构的字段_1 名字重命名为 page_depth
    */
    val zeroDF = df.withColumnRenamed("_1", "page_depth").withColumnRenamed("_2", "ak") //指定字段

    //gcs:使用zeroDF重新创建一个视图df_a
    zeroDF.createTempView("df_a") //创建视图


    //gcs:这种方法用来去重ak相同的值，之后再形成一个新的视图
    ss.sql("select distinct ak  from t_user_loyal").createTempView("df_b") //去重相同的ak


    //gcs:通过corss join 操作join (select df_b.ak app_key, df_a.page_depth from df_a CROSS)和df_b，之后创建一个新的属兔df_c
    val view = ss.sql("select df_b.ak app_key, df_a.page_depth from df_a CROSS JOIN df_b   ").createTempView("df_c")


    /*
    *gcs:
    *首先按照ak,uu,at来对数据进行分组。之后求出在当前的这个session中用户的访问页的数量
    */
    //gcs:计算不同的访问深度
    //gcs:ak,uu,at,page_depth
    ss.sql("SELECT ak app_key,uu,at, (case when count(pp)=1  then '1' " +
      "when count(pp)=2  then '2' " +
      "when count(pp)=3 then '3' " +
      "when count(pp)=4 then '4' " +
      "when count(pp)>=5 and count(pp)<=10 then '5' " +
      "else '6' end) page_depth FROM t_user_loyal where ev ='app' GROUP BY ak,uu,at")
      .createTempView("b")


    ////gcs:计算打开次数
    //gcs:"at"  : "启动时上产生的access_token"
    //==========================================================n2-1
    /*
    *gcs:
    *计算打开次数。根据at字段来进行计算
    */
    //gcs:ak,uu,at,1
    ss.sql("select ak app_key ,uu,at,count(DISTINCT at) as pps from t_user_loyal where ev ='app' group by ak,uu,at")
      .createTempView("a") //gcs:这里计算的pps 的值全部都是1


    ////gcs:打开次数与标记表连接.
    //==========================================================n2-2
    /*
    *gcs:
    *将计算出来的“打开次数”的指标的结果和访问深度进行结合的操作
    */
    //gcs:ak,uu,at,1 或者 ak,uu,at,0
    ss.sql("select b.app_key ,b.uu,b.page_depth ,a.pps from b left join a on b.app_key=a.app_key and b.uu=a.uu and b.at=a.at")
      .na.fill(0)
      .createTempView("c")
    //gcs:ak,uu,page_depth 0,1


    //==========================================================n2-3
    /*
    *gcs:
    *先把c中的数据按照ak和page_depth进行分组。之后计算uu和pps
    * 这样的结果就计算出来了。同一个ak下的每一个page_depth的访问人数和访问页面次数
    */
    ////gcs:计算不同的访问人数和打开次数
    //gcs:
    ss.sql("SELECT app_key,page_depth,count(distinct uu) as num_of_user,sum(pps) as vists_times  from c group by app_key,page_depth  ")
      .createTempView("d")   //gcs:这里的sum(pps)是将多个1进行累计的加和


    //==========================================================n2-4
    /*
    *gcs:
    *先将数据库中的数据按照ak进行分组，之后计算uu和pps
    * 这样的计算结果是同一个ak下的所有的访问人数和访问页面次数
    * MySqlSQL 中计算app_key的一个维度的和，要先将所有的离线数据按照app_key进行分组。这个意思是先按照分组形成若干个临时表，
    * 之后使用count的方法，就可以计算临时表当中的数据的结果了。
    */
    ////gcs:计算所有访问人数 和打开次数
    ss.sql("select app_key, sum (num_of_user) as users,sum (vists_times) as times from d group by app_key ")
      .createTempView("e")


    //==========================================================n2-5
    /*
    *gcs:
    *计算每一层访问深度下的
    * percent = num_of_user / e.users   每一深度下的访问人数/总的访问人数
    * t_percent =  vists_times / e.times  每一深度下的访问次数/总的访问次数
    */
    //gcs:进行SQL语句的select操作
    val t_f = ss.sql(s"SELECT d.app_key,  page_depth, num_of_user, " +
      " (cast(num_of_user as double) / cast(e.users as double)) AS percent ," +
      " vists_times, " +
      " (cast(vists_times as double) / cast(e.times as double)) AS t_percent " +
      "  from d left join e on d.app_key=e.app_key group by d.app_key,d.page_depth,d.vists_times,d.num_of_user,e.users,e.times  ").na.fill(0)
    t_f.createTempView("f")


    //gcs:这个sql语句中的使用yesterday还是非常不错的
    val result = ss.sql("select df_c.app_key," + yesterday + " day,df_c.page_depth,f.num_of_user,f.percent,f.vists_times" +
      " ,f.t_percent, NOW() as update_at from df_c  left join f on (df_c.page_depth=f.page_depth and df_c.app_key=f.app_key) ").na.fill(0)
    println("打印计算结果")
    result.show()


    //==========================================================2
    /*gcs:
    *将数据写入到aldstat_visit_depth表当中
    */
    try {
      result.foreachPartition((rows: Iterator[Row]) => {
        val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
        val sqlText = "insert into aldstat_visit_depth (app_key,day,visit_depth,visitor_count, visitor_ratio," +
          "open_count,open_ratio,update_at)" +
          "values (?,?,?,?,?,?,?,?) ON " +
          "DUPLICATE KEY UPDATE visitor_count=?, visitor_ratio=?, open_count=?,open_ratio=?"

        rows.foreach(r => {
          val app_key = r.get(0) //gcs:df_c.app_key
          val day = r.get(1) //gcs:day
          val visit_depth = r.get(2) //gcs:df_c.page_depth   访问深度
          val visitor_count = r.get(3) //gcs:f.num_of_user 当前深度下的访问人数
          val visitor_ratio = r.get(4) //gcs:f.percent 当前访问深度的访问人数/总的访问人数 比值   percent = num_of_user / e.users   每一深度下的访问人数/总的访问人数
          val open_count = r.get(5) //gcs:f.vists_times  当前访问深度下的访问次数
          val open_ratio = r.get(6) //gcs:f.t_percent   t_percent =  vists_times / e.times  当前深度下的访问次数/总的访问次数。比值 每一深度下的访问次数/总的访问次数
          val update_at = r.get(7) //gcs:NOW()     当前的时间

          params.+=(Array[Any](app_key, day, visit_depth, visitor_count, visitor_ratio, open_count, open_ratio, update_at,visitor_count,visitor_ratio,open_count,open_ratio))
        })

        JdbcUtil.doBatch(sqlText, params)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
    ss.close()
  }
}
