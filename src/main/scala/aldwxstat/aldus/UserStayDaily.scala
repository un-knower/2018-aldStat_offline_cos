package aldwxstat.aldus

//==========================================================
/*
*gcs:
*新的用户留存的代码
* -d 是补昨天的数据
* 如果补今天的7天和30天的数据。那么运行这个程序就什么参数也不用添加。
* 如果要补前天(2018-06-30)的7天和30天的数据。那么-d 后面添加的参数是"-d 2018-06-29"。
* 因为7天和30天的数据是默认是从昨天开始补的数据
*/

/*
*gcs:
*这个版本是张世坚写的版本
*/
import java.sql.Statement
import java.text.SimpleDateFormat
import java.util.Date

import aldwxutils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


object UserStayDaily {
  def main(args: Array[String]) {
    val logger = Logger.getLogger(this.getClass)
    Logger.getLogger("org").setLevel(Level.WARN)
    AldwxDebug.debug_info("2018-06-21", "zhangshijian")
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      //        .master("local[*]")
      .getOrCreate()

    //获得昨天时间
    val tt = TimeUtil.processArgs(args)
    //获得昨天时间戳
    val nowtime = TimeUtil.str2Long(tt)
    //获得传入参数的日期 +1 天

    //将long 类型转换成 string
    def long2Str(long: Long): String = {
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val data = new Date(long)
      sdf.format(data)
    }

    //==========================================================1
    /*
    *gcs:
    *创建一个时间数组。将1-7,8,15,30天，这些时间存放到timeArg这个数组当中
    */
    val timeArg = new collection.mutable.ArrayBuffer[String]()

    //==========================================================2
    /*
    *gcs:
    *以此遍历这个时间数组
    */
    for (i <- 0 to 7) {
      timeArg.append(long2Str(nowtime - 86400 * 1000 * i.toLong))
    }
    timeArg.append(long2Str(nowtime - 86400 * 1000 * 14.toLong))
    timeArg.append(long2Str(nowtime - 86400 * 1000 * 30.toLong))


//    logger.debug(s"\nread the following day directory:\n${timeArg.mkString("\n")}")



    //==========================================================3
    /*
    *gcs:
    *因为ArgsTool的默认值是“昨天”的时间。所以这个是默认读取 “昨天-0”，即昨天一天的数据
    */
    val df0 = ArgsTool.getSpecifyTencentDateDF(args, spark, Array(0))
    //    val df0: DataFrame = spark.read.parquet(s"D:\\Program\\data\\${timeArg(0)}")

    //==========================================================4
    /*
    *gcs:
    *将DF当中的ak,uu字段提取出来。
    * df0是昨天的DataFrame
    */
    val todayDf = df0.select(
      df0("ak"),
      df0("uu")
    ).filter(s"ev='app'")  //gcs:刚才的select的过程没有把ak,uu这个字段筛选出来啊，为什么这里还是根据ev进行筛选呢??

    //==========================================================5
    /*
    *gcs:
    *将todayDf做一下cache的缓存
    */
    todayDf.cache()

    //==========================================================6
    /*
    *gcs:
    *这里为什么要先把0~9天的数据读取出来呢
    */
    //    for (i <- 4 to 9) {
    for (i <- 0 to 9) {
      val df = ArgsTool.getSpecifyTencentDateDF(args, spark, Array(i))  //gcs:提取第i~9天的数据。7天和30天的旧数据
      val otherDf = df.select(
        df("ak").alias("base_ak"),
        df("uu").alias("base_uu"),
        //gcs:expr函数的意思是按照Mysql当中的操作，执行expr方法的形参中的语句。即对ifo进行case...when的操作
        expr("case when ifo='true' then 1 else 0 end").alias("base_is_first_come")   //gcs:判断在读取的这第i天的数据中，用户是否是新用户。新用户将被命名为 base_is_first_come
      )
        .filter(s"ev='app'")

      //==========================================================7
      /*
      *gcs:
      * 这个otherDF是我们分别读取到的第i天的数据。因为这里面是有一个for循环的。
      *首先进行join的操作。根据groupBy进行操作
      * 之后将这个各个ak下的第i天的新用户数读取出来
      */
      //计算当天uv,计算新增uv
      val uvDf = otherDf.groupBy("base_ak")  //gcs:将这一天的数据按照ak进行分组

        //gcs:这第i天的数据用户全部的uu为base_uu。
        //gcs:new_uv 是base_is_first_come=1 的uu的distinct的结果
        .agg(countDistinct("base_uu").alias("active_uv"), countDistinct(expr("case when base_is_first_come=1 then base_uu else null end")).alias("new_uv"))
        .select("base_ak", "active_uv", "new_uv")


      //==========================================================8
      /*
      *gcs:
      *进行join的操作
      * todayDF 是昨天的DF
      * otherDF 是读取的0~9天之前的DF
      * 将昨天的DF和第0~9天之前的步骤7 的 uvDF进行join的操作，这样就找到了同时在第0~9天的otherDF登录过的和在昨天的DF(todayDF)登陆过的用户的交集
      * 这个交集就是我们的最终的计算的结果。
      */
      val active_leftDf = todayDf.join(otherDf, otherDf("base_ak") === todayDf("ak") && otherDf("base_uu") === todayDf("uu"))
        .select("base_ak", "base_uu", "base_is_first_come")




      //==========================================================下面是今天、7天、30天的活跃的人数

      //==========================================================9
      /*
      *gcs:
      *计算新增的留存人数，并且为查出来的数据重新取名字
      * new_left 等于是"第i天的新用户"/“同时符合是‘第i天的新用户’&&'昨天的登录用户'”
      * active_left 在"第i天 登录过的用户"&&"在今天 同样登录过的用户"
      */
      //留存人数,新增留存人数
      val people_left = active_leftDf.groupBy("base_ak")
        .agg(countDistinct("base_uu").alias("active_left"), countDistinct(expr("case when base_is_first_come=1 then base_uu else null end")).alias("new_left"))
        .select("base_ak", "active_left", "new_left")
        .withColumnRenamed("base_ak", "ak")

      //==========================================================10
      /*
      *gcs:
      *将最终的结果join在一起
      */
      //关联
      val result = uvDf.join(people_left, uvDf("base_ak") === people_left("ak"), "left_outer")
        .select("base_ak", "active_uv", "new_uv", "active_left", "new_left")
        .na.fill(0)

      result.createOrReplaceTempView("v_tmp")

      //==========================================================11
      /*
      *gcs:
      * 从视图v_tmp当中读取数据，之后从这个v_tmp当中读取数据
      *base_ak 是处理数据的ak
      * day 代表当前的数据属于哪一天的数据。代表第day天的数据。PHP会根据day的时间段来读取数据。不是根据day_after来读取数据的。
      *   我们的程序在今天计算留存率的时候，先让Time今天-1...7，这样就可以形成了day。这个字段。因为我们的数据每一天都是在进行更新的。这样的计算方法就可以把每天算出留存率。
      *   这样就会每天都在更新数据。
      *   如果我想在库里面查从今天开始的7天之前的留存率，我需要在今天2018-07-27减去7，等于2018-07-20。然后让day=2018-07-20，让day_after=7，在根据ak，就可以查询出来，今天2018-07-27的第7天的留存率了
      *   为什么要使用day和day_after这个组合来查询数据呢？ 因为根据ak，day=2018-07-20 查询出来的数据，可能是2018-07-27的7天之前的7天留存率，也可能是2018-07-21的1天留存率，
      *   所以是需要进行根据day和day_after的组合来查询数据的。
      * day_after 代表，这个数据是从数据分析的那一天开始的第几天的数据，day 的after的天数
      * active_people_ratio 是一个比值。如果active_uv 等于0，此时就会赋值为0，所以这里要计算一个比值，要检测分母是否为0
      * new_people_ratio :等于第i天的新用户数和昨天的用户数做join/第i天的新用户
      */
      val day = if (i == 8) 14 else if (i == 9) 30 else i
      val rs = spark.sql(
        s"""
           |select base_ak,'${timeArg(i)}' day,$day as day_after,active_left,new_left,
           |case when active_uv=0 then 0 else cast(active_left/active_uv as float) end active_people_ratio,
           |case when new_uv=0 then 0 else cast(new_left/new_uv as float)  end  as new_people_ratio
           |from v_tmp
         """.stripMargin)


      //==========================================================12
      /*
      *gcs:
      *将最终的结果写入到MySql当中
      */
      // 写入数据库
      insert2db(rs)
    }

    spark.stop()
  }

  private def insert2db(rs: DataFrame) = {
    rs.foreachPartition((rows: Iterator[Row]) => {
      //连接
      var statement: Statement = null
      val conn = MySqlPool.getJdbcConn()
      statement = conn.createStatement()
      try {
        conn.setAutoCommit(false)
        rows.foreach(r => {
          val ak = r(0)
          val day = r(1)
          val diff = r(2)
          val people_left = r(3) //活跃用户
          val new_people_left = r(4) //新增用户
          val active_people_ratio = r(5) //活跃用户留存比
          val new_people_ratio = r(6) //新增用户留存比

          /*
          *gcs:
          *new_left 等于是"第i天的新用户"/“同时符合是‘第i天的新用户’&&'昨天的登录用户'”
          *active_left 在"第i天 登录过的用户"&&"在今天 同样登录过的用户"
          */
          val sql = s"insert into ald_stay_logs (app_key,day,day_after,new_people_left,people_left,new_people_ratio,active_people_ratio)" +
            s"values ('${ak}', '${day}', '${diff}','${new_people_left}','${people_left}','${new_people_ratio}','${active_people_ratio}') ON DUPLICATE KEY UPDATE new_people_left='${new_people_left}',people_left='${people_left}',new_people_ratio='${new_people_ratio}',active_people_ratio='${active_people_ratio}'"
          statement.addBatch(sql)
        })
        statement.executeBatch
        conn.commit()
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        if (conn != null)
          conn.close()
      }
    })
  }
}
