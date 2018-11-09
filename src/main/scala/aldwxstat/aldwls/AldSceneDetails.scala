package aldwxstat.aldwls

import java.util.Properties

import aldwxconfig.ConfigurationUtil
import aldwxutils.ArgsTool
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by wangtaiyang on 2017/12/13.
  * 这是场景值的函数
  */

object AldSceneDetails {

//  //  需要在group by 后面分组的（正常情况下就是不同的维度）
//  val dimensionAnalysis = "brand"

  /**
    * 主函数
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .appName("AldSceneDetails")
//      .master("local")
      //.config("spark.sql.shuffle.partitions", 300)
      .getOrCreate()

    //    连接数据库
//    val url = DBConf.url
//    val prop = new Properties()
//    prop.put("driver", DBConf.driver)
//    prop.setProperty("user", DBConf.user)
//    prop.setProperty("password", DBConf.password)
//    spark.read.jdbc(url, "ald_cms_scene", prop).createOrReplaceTempView("ald_cms_scene")
    val url = ConfigurationUtil.getProperty("jdbc.url") //gcs:这是获得jdbc的连接
    val prop = new Properties() //gcs:获得一个新的配置项
    //==========================================================1
    /*
    *gcs:
    *获得MySql的数据的连接
    * driver是JDBC的驱动
    * user 是JDBC的用户
    * password 是JDBC的密码
    */
    prop.put("driver", ConfigurationUtil.getProperty("jdbc.driver")) //gcs:获得jdbc的driver
    prop.setProperty("user", ConfigurationUtil.getProperty("jdbc.user"))
    prop.setProperty("password", ConfigurationUtil.getProperty("jdbc.pwd"))

    //==========================================================2
    /*
    *gcs:
    *我去，发现了新大陆，这是从 ald_cms_scene 表中将数据都读取出来的一种方式。
    * 原来spark还可以从MYSql数据库中读取数据，将数据读取出来之后就可以创建一个视图了
    */
    spark.read.jdbc(url, "ald_cms_scene", prop).createOrReplaceTempView("ald_cms_scene")
    //    将参数传入和sparksession传入，真正执行
    //==========================================================3
    /*
    *gcs:
    *开始执行程序了
    */
    execute(args, spark)
    spark.close()
  }

  /**
    * 判断执行昨日数据还是某一天的数据
    *
    * @param args
    * @param spark
    */
  def execute(args: Array[String], spark: SparkSession): Unit = {
    ArgsTool.analysisArgs(args)
    if (args.length == 0) {
      println("正常执行")
      //正常执行,从昨天开始算（包括昨天）
      fillDailyData(args,spark)
    } else if (ArgsTool.du == "" && ArgsTool.day != "") {
      println("补" + ArgsTool.day + "的数据")
      fillDailyData(args,spark)
    }
  }


  //真正执行昨天或某一天数据
  def fillDailyData(args: Array[String],spark: SparkSession): Unit = {

    //==========================================================4
    /*
    *gcs:
    *从数据中将-d num 天的数据读取出来，并且创建视图
    */
    val logs = ArgsTool.getLogs(args,spark,ConfigurationUtil.getProperty("tongji.parquet"))
    //val logs = ArgsTool.getLogs(args,spark,ConfigurationUtil.getProperty("tencent.parquet"))
    //val logs = ArgsTool.getTencentLogs(args,spark,ConfigurationUtil.getProperty("tencent.parquet"))

    println("创建原始日志视图...")
    logs.createOrReplaceTempView("logs")

    //==========================================================5
    /*
    *gcs:
    *为数据分析创建数据做准备
    * logs 是我们读取出来的原始的日志
    * ald_cms_scene 是从MySql当中读取出来的数据
    * 让这两个视图logs和ald_cms_scene 进行join的操作，join的条件是根据scene进行连接的操作。
    * 执行完之后创建视图 scene_data_all
    */
    spark.sql(s"SELECT a.ak,a.uu,a.dr,a.ifo,a.ev,a.at,a.pp,a.st,a.hour,a.scene scene,b.scene_group_id scene_group FROM logs  a left join ald_cms_scene b on a.scene = b.sid where hour !='null'")
      .createOrReplaceTempView("scene_data_all")
    //数据源分三种：app（小程序）、page（页面）、event（时间），只有分享取ev=event
    //    获取需要在ev=app中计算的指标的字段
    //==========================================================6
    /*
    *gcs:
    *从ev=app当中读取数据，创建视图 app_tmp
    */
    spark.sql("select ak,uu,at,scene,scene_group,hour,ifo,dr from scene_data_all where ev='app'")
      .createOrReplaceTempView("app_tmp")
//    spark.sql("select * from app_tmp").show()
    spark.sqlContext.cacheTable("app_tmp")
    //    获取需要在ev=page中计算的指标的字段

    //==========================================================7
    /*
    *gcs:
    *从ev=page 当中读取ev=page当中的数据，同时创建视图
    */
    spark.sql("select ak,uu,scene,scene_group,hour,pp,at from scene_data_all where ev='page'")
      .createOrReplaceTempView("visitor_page")

    spark.sqlContext.cacheTable("visitor_page")

    //==========================================================8
    /*
    *gcs:
    *创建指标计算方式
    * 这里的 笔记7 将原始数据读取出来的过程，我们可以加上维度的过程啊。之后就可以专门的写一个指标的计算类了，将维度筛选过程中形成的视图，传入
    * 这个指标计算的类当中，之后就可以用来计算指标的了。
    * 这里要注意一个问题，被调函数在编译的时候，被调函数中的代码会被拷贝到主调函数当中
    * 我在 笔记7 的位置将维度需要计算的数据先根据ev和维度字段 提取出来
    * 之后像 笔记8 的那样，create several  PublicModularDetails,which is used to compute the date and create the Tem
    * at last,put the Tem name to the insertTable function ,then u can used the Tem as the connection between two function of note 8 & note9,putting
    * the date to the database
    */
    //    创建对象，将sparksession和需要执行的维度出进去
    val sceneDailyDetails: PublicModularDetails = new PublicModularDetails(spark, "scene")
    val sceneGroupDailyDetails: PublicModularDetails = new PublicModularDetails(spark, "scene_group")
    val hourSceneDailyDetails: PublicHourModularDetails = new PublicHourModularDetails(spark, "scene")
    val hourSceneGroupDailyDetails: PublicHourModularDetails = new PublicHourModularDetails(spark, "scene_group")

    //==========================================================9
    /*
    *gcs:
    *将要分析的指标类传进去，进行数据的分析
    */
    //    调用最终执行的接口函数
    adDailyData(sceneDailyDetails)
    //==========================================================10
    /*
    *gcs:
    *将数据分析的结果，插入到数据库。
    * 插入到Mysql数据库的操作，其实还是应该，每一个维度都单独写一个的，因为你要从指标类 PublicModularDetails 中将所有的指标的计算结果都聚合起来，还是涉及到很多的视图的操作的
    * 所以最好将指标集合的方法和比率的计算方法都放在insertMySql的函数当中
    * 这里MYSql的插入操作，用不用使用Kafka做一个缓冲呢？
    */
    new PublicMysqlInsertDetails(spark).sceneInsert2db()

    adDailyData(sceneGroupDailyDetails)
    new PublicMysqlInsertDetails(spark).sceneGroupInsert2db()
    hourAdDailyData(hourSceneDailyDetails)
    new PublicMysqlInsertDetails(spark).hourSceneInsert2db()
    hourAdDailyData(hourSceneGroupDailyDetails)
    new PublicMysqlInsertDetails(spark).hourSceneGroupInsert2db()
    //    清除缓存表
    spark.sqlContext.uncacheTable("app_tmp")
    spark.sqlContext.uncacheTable("visitor_page")
  }

  /**
    * 对创建的类进行实现
    *
    * @param sceneAnalysis
    */
  def adDailyData(sceneAnalysis: PublicModularDetails): Unit = {
    sceneAnalysis.newUser()
    sceneAnalysis.visitorCount()
    sceneAnalysis.openCount()
    sceneAnalysis.totalPageCount()
    sceneAnalysis.totalStayTime()
    sceneAnalysis.secondaryAvgStayTime()
    sceneAnalysis.visitPageOnce()
    sceneAnalysis.bounceRate()
  }

  /**
    * 对创建的类进行实现（小时）
    *
    * @param hourAnalysis
    */
  def hourAdDailyData(hourAnalysis: PublicHourModularDetails): Unit = {
    hourAnalysis.newUser()
    hourAnalysis.visitorCount()
    hourAnalysis.openCount()
    hourAnalysis.totalPageCount()
    hourAnalysis.totalStayTime()
    hourAnalysis.secondaryAvgStayTime()
    hourAnalysis.visitPageOnce()
    hourAnalysis.bounceRate()
  }
}
