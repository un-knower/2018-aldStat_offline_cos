//==========================================================f5
//gcs:用户留存率 模块代码

package aldwxstat.aldus

import java.net.URI
import java.sql.Statement

import aldwxconfig.ConfigurationUtil
import aldwxutils._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer


@deprecated
/**
  * <b>author:</b> gcs <br>
  * <b>data:</b> 18-5-7 <br>
  * <b>description:</b><br>
  *   用户留存率功能模块的7天,30天数据 <br>
  * <b>param:</b><br>
  * <b>return:</b><br>
  */
object UserStayDaily_Old {

  /**
  * <b>author:</b> gcs <br>
  * <b>data:</b> 18-5-7 <br>
  * <b>description:</b><br>
    *   用户留存率的主程序
  * <b>param:</b><br>
  * <b>return:</b><br>
  */
  def main(args: Array[String]) {

    val logger = Logger.getLogger(this.getClass)
    Logger.getLogger("org").setLevel(Level.WARN)

    //==========================================================1

    AldwxDebug.debug_info("2017-11-06", "zhangyanpeng")

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .getOrCreate()


    //==========================================================2
    /*gcs:
    *俊翔 提到过，这个位置如果不加-d 这个参数，，可以成功地解析，昨天，7天和30天的数据。这是因为day的缺省值就是昨天的日期
    * 可是俊翔说加了 -d 2017-05-03  只跑7天和30天的数据,那是因为，-d指定了日期之后，是分析 从2017-05-03开始的第(-30,-14,-7,-6,-5,-4,-3,-2,-1,-0)的数据
    * 什么参数都不添加的话之间是计算昨天，7天，30天的数据
    * 对于参数的分析，关键就在 processArgs 这个函数当中了
    *   加了数据之后，补的数据就是从day天开始的数据。但是PHP在提取数据的时候，会根据day这个参数来提取数据。shu ju you wen ti. jiu hui bei bu cuo de.
    */
    //gcs:解析args参数中给定的时间。如果没有指定时间，将自动获取昨天的时间获得昨天时间
    val tt= TimeUtil.processArgs(args) //gcs:根据传入的参数获得昨天的日期数据。tt的值等于 “-d 2018-08-10” 中的20180810

    //gcs:将tt转换成long类型的数据
    val nowtime = TimeUtil.str2Long(tt) //gcs:把用户的String类型的数据转换成long类型的秒数据。这个nowtime是代表昨天的数据

    //gcs:获得传入“-d xxxx”的xxxx日期 +1 天。
    //gcs:argTime 代表 xxxx这一天结束的最后的时刻。即使用xxxx的后一天（xxxx+1）的凌晨时间来作为xxxx的结束的时间
    val argTime = nowtime+86400*1000.toLong //gcs:86400单位为ms，代表一天有86400ms。86400*1000 代表一天的意思。 //作者的注释：1天后的时间戳，截止到昨日凌晨
    val arg7 = argTime-86400*1000*8.toLong //gcs:arg7 就是"-d xxxx"中的xxxx的往回数第7天的日期 //来自作者：7天前的时间戳,前第7天的凌晨0点

    //获得前15天的时间戳，why是前15天的数据呢  //gcs:按照arg7的原理，这里应该是获得前14天的数据啊？？？？？
    val arg15 = argTime-86400*1000*15.toLong //gcs:获得xxxx的往前数的第15天的凌晨的日期。 如果xxxx是2018-05-11，那么arg15就是从2018-05-11,2018-05-10,2018-05-09....的2018-04-28，即arg15等于2018-04-26

    //获得前14天的时间戳
    val arg14 = argTime-86400*1000*14.toLong //gcs:获得xxxx的往前数的第14天的凌晨的日期

    //29天和三十天 之前的时间戳
    val arg31 = argTime-86400*1000*31.toLong  //gcs:获得xxxx的往前走的第30天的时间戳
    val arg30 = argTime-86400*1000*30.toLong //gcs:获得xxxx的往前走的第29天的时间戳


    //创建文件配置对象
    val conf = new Configuration()
    val uri =URI.create(s"${ConfigurationUtil.getProperty("tencent.parquet")}")//hdfs的路径 //gcs: "tencent.parquet" = cosn://aldwxlogbackup/log_parquet


    //获取文件对象
    val fs = FileSystem.get(uri,conf)

    // 读取的文件
    val json_files = ArrayBuffer[String]()
    val day_filter = ArrayBuffer[String]()


    val daysArr=Array(30,14,7,6,5,4,3,2,1,0)


//    for (j <- 0 until(dayArr.length)){
//      val jTime =  nowtime - (dayArr(j) * TimeUtil.day.toLong)
//      val jDay = TimeUtil.long2Str(jTime)
//      day_filter.append(jDay)
//      //判断最小天数  路径是否存在
//      if (fs.isDirectory(new Path(s"${ConfigurationUtil.getProperty("tencent.parquet")}/$jDay"))){
//        //循环 读取文件
//        val path = new Path(s"${ConfigurationUtil.getProperty("tencent.parquet")}/$jDay")
//        json_files+= (path.toString + "/*/part-*")
//        println(s"${ConfigurationUtil.getProperty("tencent.parquet")}/$jDay")
//      }else{
//        println(s"${ConfigurationUtil.getProperty("tencent.parquet")}/$jDay")
//        logger.warn(s"没有最大时间${dayArr(j)} 的路径。。。")
//      }
//    }


    //==========================================================2
    //gcs:获得EMR指定天数的数据。第args的（-30，-14,-7,-6,-5,-4,-3,-2,-1,-0） 天的数据
    //gcs:getSpecifyTencentDateDF是把指定天数的指定的ak的数据提取提取出来，之后使用filter操作是将这些提取出来的数据，提取出来之后还继续做了一个ev == app的判断，将ev不等于app上报类型的数据都筛选出去了
    val file: DataFrame = ArgsTool.getSpecifyTencentDateDF(args, spark, daysArr).filter(col("ev") === "app")



    //读取文件  [今天日期，30天前日期)
    //val file: DataFrame =spark.read.parquet(json_files:_*).filter(col("ev")==="app" )
    //重新分区
    //val re_file = Aldstat_args_tool.re_partition(file,args)

    //==========================================================3
    //gcs:获得"-d xxxx"期间的xxxx~xxxx-6 即往回数包括xxxx在内的7天的数据的每一天的数据。因为 （-30，-14,-7,-6,-5,-4,-3,-2,-1,-0）中就是包括0,-1,-2,-3,-4,-5,-6,-7期间的数据
    val df1: DataFrame =file.filter(s"st<'$argTime' and st>='$arg7'") //gcs:st字段 比如值可以为 1521096783191，st字段记录着微信小程序的页面被打开的时间

    //==========================================================4
    //gcs:获得 前第14天的一整天的数据。要注意的是，这个第14指的是两个时间节点之间间隔。如果"-d 2018-05-10" 即xxxx等于2018-05-10的话，第14是2015-04-26(第15天)~2015-04-27（第14天）的时间间隔。具体的图，看印象笔记
    val df2: DataFrame =file.filter(s"st<'$arg14' and st>='$arg15'") //gcs:arg14其实是xxxx的往前数的第13天的日期；arg15其实是xxxx往前数的第14天的日期

    //==========================================================5
    //gcs:获得 前第30天的一整天的数据
    val df3: DataFrame =file.filter(s"st<'$arg30' and st>='$arg31'")

    //==========================================================6
    /*gcs:
    *将提取到的df1,df2,df3进行union合并
    */
    val df: DataFrame =df1.union(df2).union(df3)
    //val df: DataFrame =spark.read.json("C:\\Users\\zhangyanpeng\\Desktop\\log1").filter(col("ev")==="app").filter("ak!=''").filter(col("st")>nowtime)




    //gcs:什么算是新用户。在nowDate那一天当中的新注册的用户就算是新用户了。那么在nowData那天注册的用户，在之后的7天，30天，再次访问了这个微信

    //新用户人数
    //gcs:利用 "ifo='true'" 把ak下的微信小程序筛选出来
    val daily_people_df = df.filter("ifo='true'") //gcs:ifo 这是指用户第一次使用该微信小程序，即该用户是一个新用户。当ifo等于1时，代表该用户是一个新用户
      //gcs:对df进行筛选之后，就会将所有的新用户都筛选出来了。
      .select(
        df("ak").alias("base_app_key"), //gcs:将ak取出来，区别名为base_app_key。这个新的用户刚刚注册了哪一个ak
        from_unixtime((df("st")/1000),"yyyy-MM-dd").alias("base_day"),//gcs:利用df("st")提取出来的时间是long类型的ms，除以1000，就把ms转换成了“秒”单位，再转换成yyyy-MM-dd的格式。利用from_unixtime函数，将st的格式转换为 yyyy-MM-dd 的格式。
        df("uu").alias("base_uuid"),   //gcs:将新用户的uu 重新命名成base_uuid
        df("ifo").alias("base_is_first_come")   //gcs:把新用户的ifo ，重新命名成 base_is_first_come
      )



    //统计活跃用户 （ifo =‘’）
    val day_after_df = df.select(
      df("ak").alias("app_key"),
      from_unixtime((df("st")/1000),"yyyy-MM-dd").alias("day_after"),   //gcs:把st这个ms的时间转化成2018-05-13 格式的时间。st是用户的访问这个页面的时间
      df("uu").alias("day_after_uuid"),
      df("ifo").alias("day_after_is_first_come") //gcs:ifo 存储的是用户是否是新的用户
    ).cache()



    //两个表进行join （笛卡尔积）  新用户。
    // gcs:新增用户的用户活跃度.即新增留存。先把新的用户提取出来，再把所有的活跃的用户提取出来，之后将新用户和留存
    /*gcs:计算方法
    *将新增的用户和统计出来的活跃的用户进行笛卡尔积，这样的话就可以用来计算所有的新用户的活跃度了。
    */
    //gcs: _result_df 存储的就是初步的新用户的留存表
    val _result_df = daily_people_df.crossJoin( //gcs:先使用day_after_df.select 把要提取的字段提取出来成为一个DataSet，之后再把这个DataSet与daily_people_df进行笛卡尔积操作
      day_after_df.select(
        "app_key",   //gcs:收集到微信小程序的app_key
        "day_after", //gcs:把st转换成2018-05-13的时间的格式，之后把数据提取出来
        "day_after_uuid" //gcs: 把使用微信小程序的uuid提取出来
      ))




    //gcs:从_result_df 当中提取数据。
    /*gcs:
    *这个提取的数据就是新用户的留存率
    */
    //gcs:从新用户的留存表中的有用的字段都提取出来
    val _re_df = _result_df.select(
      _result_df("base_app_key"),           //新增ak
      _result_df("base_day"),               //新增day  新增用户的st转换成了2018-05-15 的形式

      _result_df("base_uuid"),              //新增用户uu
      _result_df("base_is_first_come"),     //新增用户的ifo信息
      _result_df("day_after"),              //天数
      _result_df("day_after_uuid"),         //活跃用户uu
      datediff(_result_df("day_after"), _result_df("base_day")).alias("diff") //返回两个日期之间的间隔。计算新的用户从注册那天起，到现在的这个时间的间隔。之后为这个时间间隔取了一个新的名字，叫做diff。新用户注册和留存的用户的数据是不一样的
    ) .filter("base_uuid==day_after_uuid")  //gcs:将新增用户的uuid和活跃用户的uuid相等地筛选出来。这样就可以知道新增用户并且是活跃用户的人数了
      .distinct()


    //gcs:这是统计的新用户的在30天，7天，6天的新的用户
    val _r_df = _re_df.select(
      _re_df("base_app_key"),
      _re_df("base_day"),
      _re_df("diff"),
      when(_re_df("base_is_first_come").isNull, 0).otherwise(1).alias("is_first_come")
    ).groupBy("base_app_key", "base_day", "diff")
      //不分组聚合
      .agg(
      sum("is_first_come") as "new_people_left"
    ).na.fill(0) //gcs:利用na函数将为null的列提取出来，之后在利用fill，把为0的列赋值为0
      //.withColumn("new_people_left", lit(0)) //拼接到后面的一列
      //gcs:这里根据diff就可以筛选出新用户在30,14,7,6...天的留存的数目了
      .filter(column("diff")===30 || column("diff")===14 || column("diff")===7 || column("diff")===6 || column("diff")===5 || column("diff")===4 || column("diff")===3 || column("diff")===2 || column("diff")===1 || column("diff")===0) //gcs:过滤




    //==========================================================
    /*gcs:
    *接下来是 “活跃留存”。新用户和老用户都会统计到活跃留存当中
    */
    //-------------------------------------
    //gcs:在args的统计时间内的 活跃用户。统计所有的用户的数据
    val active_people_df =df.select( //gcs:df是收取出来的7,14,30天的所有的数据
      df("ak").alias("active_app_key"), //gcs:active_app_key 是为我们所有的用户在使用哪一个ak
      from_unixtime((df("st")/1000),"yyyy-MM-dd").alias("active_day"), //gcs:将Long类型的的st转换成了 2018-05-15 的时间的格式
      df("uu").alias("active_uuid")
      //df("ifo").alias("active_is_first_come")
    )

    //两个表进行join （笛卡尔积）  统计时间内的 活跃用户
    val active_result_df = active_people_df.crossJoin(
      day_after_df.select(  //gcs:day_after_df 是存储着用户的留存率
        "app_key",
        "day_after",
        "day_after_uuid"
      ))



    //gcs:计算所有的用户的活跃度的信息
    //gcs:active_re_df 其实就是所有的用户的活跃度的信息了
    val active_re_df = active_result_df.select(
      active_result_df("active_app_key"),           //活跃用户ak
      active_result_df("active_day"),               //活跃用户day 。用户在哪一天登录了这个微信小程序
      active_result_df("active_uuid"),              //活跃用户uu
      //active_result_df("active_is_first_come"),     //ifo
      active_result_df("day_after"),              //天数
      active_result_df("day_after_uuid"),         //活跃用户uu   day_after_uuid其实是uu的别名
      datediff(active_result_df("day_after"), active_result_df("active_day")).alias("diff") //用户在登录的那一天起到重新登录的那一天，之间的时间的间隔
    ) .filter("active_uuid==day_after_uuid")
      .distinct()



    //gcs:将所有的用户的活跃度的信息中有用的数据都提取出来
    val active_r_df = active_re_df.select(
      active_re_df("active_app_key"),
      active_re_df("active_day"),
      active_re_df("diff"),
      active_re_df("day_after_uuid")
      //when(active_re_df("active_is_first_come").isNull, 1).otherwise(1).alias("active_come")
    ).groupBy("active_app_key", "active_day", "diff")  //gcs:将用户按照app_key，
      //不分组聚合
      .agg(
      countDistinct("day_after_uuid")  as "people_left"
    ).na.fill(0)
      //.withColumn("new_people_left", lit(0)) //拼接到后面的一列
      .filter(column("diff")===30 || column("diff")===14 || column("diff")===7 || column("diff")===6 || column("diff")===5 || column("diff")===4 || column("diff")===3 || column("diff")===2 || column("diff")===1 || column("diff")===0) //过滤




    /*val rs_df = active_r_df.join(_r_df,active_r_df("active_app_key") === _r_df("base_app_key") &&
      active_r_df("active_day") === _r_df("base_day") &&
      active_r_df("diff") === _r_df("diff")
    ).select(
      active_r_df("active_app_key"),
      active_r_df("active_day"),
      active_r_df("diff"),
      active_r_df("people_left"),
      _r_df("new_people_left")
    )*/



    //gcs:这里为什么要使用active_r_df 和新用户的 _r_df 进行join的操作呢？？？？
    val rs_df = active_r_df.join(_r_df,active_r_df("active_app_key") === _r_df("base_app_key") &&  //gcs:active_r_df 是用户的活跃度。_r_df 是新用户的
      active_r_df("active_day") === _r_df("base_day") &&
      active_r_df("diff") === _r_df("diff")
    ).select(
      active_r_df("active_app_key") alias("ak"),
      active_r_df("active_day") alias("day"),
      active_r_df("diff") alias("time"),
      active_r_df("people_left") alias("pleft"),
      _r_df("new_people_left") alias("npleft")
    )

    val df_1 =rs_df.filter(rs_df("time")===0)
      .select(
        rs_df("ak") alias("r_ak"),
        rs_df("day") alias("r_day"),
        rs_df("npleft") alias("r_npleft"),
        rs_df("pleft") alias("r_pleft")
      )


    df_1.join(rs_df,rs_df("ak")===df_1("r_ak") && rs_df("day") === df_1("r_day"))
      .select(
        rs_df("ak"),
        rs_df("day"),
        rs_df("time"),
        rs_df("pleft"),
        rs_df("npleft"),
        df_1("r_npleft"),
        df_1("r_pleft")
      ).createTempView("table")


    //==========================================================5-1
    /*gcs:
    *从table当中读取数据
    */
    val rs = spark.sql("select ak,day,time,pleft,npleft,cast(pleft/r_pleft as float),cast(npleft/r_npleft as float) from table")

    var statement:Statement=null

    //==========================================================6
    //gcs:将前边的数据全部都写入到数据库当中
    // 写入数据库
    // 逐行入库
    rs.foreachPartition((rows:Iterator[Row]) => {
      //连接
      val conn =MySqlPool.getJdbcConn() //gcs:根据服务器连接MySql的服务器
      statement = conn.createStatement()
      try {
        conn.setAutoCommit(false)
        rows.foreach(r => {
          val ak = r(0)  //gcs:r(0)的值就是上面的 select ak,day,time,pleft,npleft,cast(pleft/r_pleft as float),cast(npleft/r_npleft as float) from table 当中的ak
          val day = r(1)
          val diff = r(2)
          val people_left = r(3)        //活跃用户
          val new_people_left = r(4)    //新增用户
          val active_people_ratio=r(5)   //活跃用户留存比
          val new_people_ratio=r(6)      //新增用户留存比


          //==========================================================7
          /*gcs:
          *这里的显示，7,14,30,的数据都存储在 ald_stay_logs 数据库当中了
          */
          val sql = s"insert into ald_stay_logs (app_key,day,day_after,new_people_left,people_left,new_people_ratio,active_people_ratio)" +
            s"values ('${ak}', '${day}', '${diff}','${new_people_left}','${people_left}','${new_people_ratio}','${active_people_ratio}') ON DUPLICATE KEY UPDATE new_people_left='${new_people_left}',people_left='${people_left}',new_people_ratio='${new_people_ratio}',active_people_ratio='${active_people_ratio}'"
          statement.addBatch(sql)
        })
        statement.executeBatch
        conn.commit()

      }catch {
        case e: Exception => e.printStackTrace()
          conn.close()
      }
    })

    spark.stop()
  }
}
