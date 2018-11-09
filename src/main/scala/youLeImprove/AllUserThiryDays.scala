package youLeImprove

import java.sql.Timestamp
import java.util.Properties

import aldwxconfig.ConfigurationUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer
import aldwxutils._

//Thirty
/** HLL_Share_oc(list(items))
  *
  * Created by 18510 on 2017/8/31.
  */
object AllUserThiryDays {
  def main(args: Array[String]): Unit = {
    /**
      * 对一个时间段的日志数据进行读取
      */
    Logger.getLogger("org").setLevel(Level.WARN)
    AldwxDebug.debug_info("2017-11-06", "weilongsheng")
    val sparkseesion = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .getOrCreate()

    //==========================================================1
    /*
    *gcs:
    *游乐在df读取数据的时候所有的程序都是一样的
    * 但是，之后他使用了一个select的语句，将所有的没有用没有用的字段的数据都筛选出来。之后再去创建这个sum_tables这个表，然后把这个表cache一下
    */
    //读取一星期的数据
    //val df = ArgsTool.getLogs(args, sparkseesion, ConfigurationUtil.getProperty("tongji.parquet"), 30.toString)
    val df_tmp=ArgsTool.getTencentSevenOrThirtyDF(args,sparkseesion,"30")
    df_tmp.createTempView("df_tmp")
    val df = sparkseesion.sql("select ak,ev,uu,at,pp,scene,ag_aldsrc,qr,ifp,st,pm,province,city from df_tmp")
    df.createTempView("sum_tables")
    sparkseesion.sqlContext.cacheTable("sum_tables")
    /**
      * -------------------------趋势分析、手机、场景值的数据源------------------------
      */
    val df_sum = df.filter("ev = 'app' and ak != '' and ak != 'null'")
    val UpDataTime = new Timestamp(System.currentTimeMillis())
    df_sum.createTempView("senven_tmp")
    //
    //    //连接数据库
    //    val url = DBConf.url
    //    //val conn = JdbcUtil.getConn()
    //    val prop = new Properties()
    //    prop.put("driver", DBConf.driver)
    //    prop.setProperty("user", DBConf.user)
    //    prop.setProperty("password", DBConf.password)
    val url = ConfigurationUtil.getProperty("jdbc.url")
    val prop = new Properties()
    prop.put("driver", ConfigurationUtil.getProperty("jdbc.driver"))
    prop.setProperty("user", ConfigurationUtil.getProperty("jdbc.user"))
    prop.setProperty("password", ConfigurationUtil.getProperty("jdbc.pwd"))
    //新用户数+访问人数+访问次数+打开次数+次均停留时长+跳出率
    //计算手机品牌、手机型号
    val name = sparkseesion.read.jdbc(url, "phone_model", prop)
    name.cache().count()
    name.createTempView("phonename")
    //连接表（包含phone_model中的brand）
    val phone_brand = sparkseesion.sql("SELECT a.ak ak,a.uu uu,a.scene scene,b.name name,b.brand brand FROM senven_tmp a left join phonename b on a.pm = b.uname").distinct()
    //    phone_brand.show()
    phone_brand.createTempView("phone_brand")
    val values_type = sparkseesion.read.jdbc(url, "ald_cms_scene", prop)
    values_type.cache().count()
    values_type.createTempView("scene_tmp")
    val result = sparkseesion.sql("select a.ak app_key,a.uu uu,a.scene scene,a.name model,a.brand brand,b.scene_group_id from phone_brand a left join scene_tmp b on a.scene = b.sid").distinct()
    //        result.show(100)
    result.createTempView("result")

    /**
      * -------------------------二维码数据源------------------------
      */
    // 获取 ald_code 的信息, ald_code 中的 qr_group_key 只在创建时写入一次


    val qr_info_df = JdbcUtil.readFromMysql(sparkseesion, "(select app_key,qr_key,qr_group_key from ald_code) as code_df")
    qr_info_df.cache().count()
    //  qr_info_df.show()
    qr_info_df.createTempView("qr_info_df")
    // 判断某天是否上报二维码数据
    if (!df.columns.contains("ag_aldsrc")) return
    // 二维码数据源
    val page_df = df.filter("ev='page' and ag_aldsrc !='null' and qr !='null'")
      .select(
        df("ak").alias("app_key"),
        df("qr").alias("qr_key"),
        df("at"),
        df("uu")
      ).distinct()
    //    page_df.show()
    page_df.createTempView("page_df")
    // 二维码组每小时的扫码人数,扫码次数
    val grouped_df = qr_info_df.join(
      page_df,
      qr_info_df("app_key") === page_df("app_key") &&
        qr_info_df("qr_key") === page_df("qr_key")
    ).select(
      qr_info_df("app_key"),
      qr_info_df("qr_group_key"),
      qr_info_df("qr_key"),
      page_df("uu")
    )
    grouped_df.createTempView("result_qr")
    //    grouped_df.show()
    /**
      * ------------------------------入口页和受访页-------------------------------
      */
    //受访页
    df.filter("ev ='page'")
      .select(
        df("ak").alias("app_key"),
        df("pp").alias("respondents"),
        df("uu")
      ).createTempView("result_respondents_page")
    //省市分析
    df.filter("ev ='app'")
      .select(
        df("ak").alias("app_key"),
        df("province").alias("province"),
        df("city").alias("city"),
        df("uu")
      ).createTempView("result_province_page")

    // 入口页
    df.filter("ev ='page' and ifp = 'true'")
      .select(
        df("ak").alias("app_key"),
        df("pp").alias("entrance"),
        df("uu")
      ).createTempView("result_entrance_page")

    /**
      * --------------------------------忠诚度---------------------------------
      */
    //访问深度
    sparkseesion.sql("SELECT ak app_key,uu, (case when count(pp)=1  then '1' " +
      "when count(pp)=2  then '2' " +
      "when count(pp)=3 then '3' " +
      "when count(pp)=4 then '4' " +
      "when count(pp)>=5 and count(pp)<=10 then '5' " +
      "else '6' end) page_depth FROM sum_tables where ev ='page' GROUP BY ak,uu")
      .createTempView("df_depth_pp")
    //计算打开次数
    sparkseesion.sql("select ak app_key ,uu,count(at) as pps from sum_tables where ev ='app' group by ak,uu")
      .createTempView("df_depth_at")
    //打开次数与标记表连接
    sparkseesion.sql("select b.app_key app_key,b.uu uu,b.page_depth visit_depth,a.pps from df_depth_pp b left join df_depth_at a on b.app_key=a.app_key and b.uu=a.uu")
      .na.fill(0)
      .createTempView("df_depth_result")
    //计算不同维度的访问人数和打开次数
    //    val depth_result_tmp = sparkseesion.sql("SELECT app_key,visit_depth,count(0) as num_of_user,   sum(pps) as vists_times  from df_depth_result group by app_key,visit_depth")
    //    depth_result_tmp.createTempView("result_depth")
    //    depth_result_tmp.show()


    //频次
    sparkseesion.sql("SELECT ak app_key,uu,(case when count(at)=1  then '1' " +
      "when count(at)=2  then '2' " +
      "when count(at)=3 then '3' " +
      "when count(at)=4 then '4' " +
      "when count(at)>=5 and count(at)<=10 then '5' " +
      "when count(at)>=11 and count(at)<=20 then '6' " +
      "when count(at)>=21 and count(at)<=30 then '7' " +
      "else '8' end) app_depth FROM sum_tables where ev ='app'  GROUP BY ak,uu").createTempView("df_frequency_at")
    //查询所有的 pp 页面访问次数
    sparkseesion.sql("SELECT ak app_key,uu,count(pp) as pps   FROM sum_tables WHERE ev='page'  GROUP BY ak,uu").createTempView("df_frequency_pp")
    //连接 标记频次表 相同uu 和ak 作为连接
    sparkseesion.sql("SELECT a.app_key,a.uu,a.app_depth visit_frequency,b.pps FROM df_frequency_at a LEFT JOIN df_frequency_pp b on a.app_key = b.app_key AND a.uu= b.uu")
      //      .na.drop(Array("pps"))
      .na.fill(0)
      .createTempView("df_frequency_result")

    //计算 用户访问时长 用最大值减去最小值
    sparkseesion.sql("SELECT ak,uu,cast(max(st)/1000 as int)-cast(min(st)/1000 as int) p_dr FROM sum_tables WHERE ev='page' GROUP BY ak,uu").createTempView("time_tmp")
    val t = sparkseesion.sql("SELECT ak app_key,uu, (case when (p_dr >=0 and p_dr<=2)  then '1' " +
      "when (p_dr >=3 and p_dr<=5)   then '2' " +
      "when (p_dr >=6 and p_dr<=10)  then '3' " +
      "when (p_dr >=11 and p_dr<=20)  then '4' " +
      "when (p_dr >=21 and p_dr<=30)  then '5' " +
      "when (p_dr >=31 and p_dr<=50)  then '6' " +
      "when (p_dr >=51 and p_dr<=100)  then '7' " +
      "else '8' end) dr_pepth FROM time_tmp  GROUP BY ak,uu,p_dr").createTempView("duration_time")
    //ak uu分组查询访问pp的次数
    sparkseesion.sql("SELECT ak app_key,uu,count(at) as pps   FROM" +
      " sum_tables WHERE ev='page' GROUP BY ak,uu").createTempView("duration_at")
    //连接访问时长标记表表
    sparkseesion.sql("SELECT b.app_key,b.uu,b.dr_pepth visit_duration,c.pps FROM duration_time b LEFT JOIN duration_at c ON b.app_key = c.app_key AND" +
      " b.uu = c.uu").createTempView("df_duration_result")

    //需要不同维度分组字段
    /**
      * 1.app_key 是统计趋势分析的七天访问人数
      * 2.app_key,scene type_value 是统计单个场景值的七天访问人数
      * 3.app_key,scene_group_id type_value 是统计趋势分析的七天访问人数
      * 4.app_key,model type_value 是统计趋势分析的七天访问人数
      * 5.app_key,brand type_value 是统计趋势分析的七天访问人数
      * 6.app_key，qr_key type_value 是统计趋势分析的七天访问人数
      * 7.app_key,qr_group_key type_value 是统计趋势分析的七天访问人数
      * 8.app_key,province type_value 是统计趋势分析的七天访问人数
      * 9.entrance入口页
      * 10.respondent受访页
      * 11.visit_depth访问深度
      * 12.visit_frequency访问频次
      * 13.visit_duration访问时长
      */


    /*--------------------------------需要添加的指标-------------------------------*/

    val arr = Array("app_key", "app_key,scene type_value", "app_key,scene_group_id type_value", "app_key,qr_key type_value", "app_key,qr_group_key type_value", "qr_key_all", "qr_group_key_all", "app_key,model type_value", "app_key,brand type_value", "app_key,province type_value", "app_key,city type_value", "app_key,entrance type_value", "app_key,respondents type_value", "app_key,visit_depth type_value", "app_key,visit_duration type_value", "entrance", "respondents")
    for (tmp_args <- arr) {
      var tmp_args_a = ""
      var lit_tmp = ""
      var gr = ""
      var re = ""
      if (tmp_args == "app_key" || tmp_args == "entrance" || tmp_args == "respondents" || tmp_args == "qr_key_all" || tmp_args == "qr_group_key_all") {
        gr = "app_key"
        if (tmp_args == "app_key") {
          re = "result"
          lit_tmp = "trend"
          tmp_args_a = "app_key"
        } else if (tmp_args == "entrance") {
          tmp_args_a = "app_key"
          re = "result_entrance_page"
          lit_tmp = "entrance_all"
        } else if (tmp_args == "respondents") {
          tmp_args_a = "app_key"
          re = "result_respondents_page"
          lit_tmp = "respondents_all"
        } else if (tmp_args == "qr_key_all") {
          tmp_args_a = "app_key"
          re = "page_df"
          lit_tmp = "qr_key_all"
        } else if (tmp_args == "qr_group_key_all") {
          tmp_args_a = "app_key"
          re = "result_qr"
          lit_tmp = "qr_group_key_all"
        }
      } else {
        lit_tmp = tmp_args.split(",")(1).split(" ")(0)
        gr = tmp_args.split(" ")(0)
        if (tmp_args.split(",")(1).split(" ")(0) == "qr_key") {
          re = "result_qr"
          tmp_args_a = tmp_args
        } else if (tmp_args.split(",")(1).split(" ")(0) == "qr_group_key") {
          re = "result_qr"
          tmp_args_a = tmp_args
        } else if (tmp_args.split(",")(1).split(" ")(0) == "province" || tmp_args.split(",")(1).split(" ")(0) == "city") {
          tmp_args_a = tmp_args
          re = "result_province_page"
        } else if (tmp_args.split(",")(1).split(" ")(0) == "entrance") {
          tmp_args_a = tmp_args
          re = "result_entrance_page"
        } else if (tmp_args.split(",")(1).split(" ")(0) == "respondents") {
          tmp_args_a = tmp_args
          re = "result_respondents_page"
        } else if (tmp_args.split(",")(1).split(" ")(0) == "visit_depth") {
          tmp_args_a = tmp_args
          re = "df_depth_result"
        } else if (tmp_args.split(",")(1).split(" ")(0) == "visit_frequency") {
          tmp_args_a = tmp_args
          re = "df_frequency_result"
        } else if (tmp_args.split(",")(1).split(" ")(0) == "visit_duration") {
          tmp_args_a = tmp_args
          re = "df_duration_result"
        } else {
          tmp_args_a = tmp_args
          re = "result"
        }
      }
      //访问人数(visitor_count)
      val visiter_count_result = sparkseesion.sql(s"SELECT ${tmp_args_a},COUNT(DISTINCT uu) visitor_count FROM ${re} GROUP BY ${gr}").withColumn("day", lit(TimeUtil.processArgs(args))).withColumn("type", lit(lit_tmp)).withColumn("update_at", lit(UpDataTime))

      // 批量入库
      alluserthirtydaysForeachPartition(visiter_count_result)
    }
    sparkseesion.stop()
  }

  private def alluserthirtydaysForeachPartition(userDf: DataFrame): Unit = {
    // 逐行入库
    userDf.foreachPartition((rows: Iterator[Row]) => {
      val params = new ArrayBuffer[Array[Any]]()
      val sqlText = "insert into ald_thirty_user_tmp (app_key,day,type,type_value,visitor_count,update_at) values (?,?,?,?,?,?)" +
        " ON DUPLICATE KEY UPDATE update_at=?, visitor_count=?"

      for (r <- rows) {

        if (r.length == 5) {
          val app_key = r.get(0)
          val visitor_count = r.get(1)
          val date = r.get(2)
          val ty = r.get(3)
          val update = r.get(4)
          val type_value = ""
          params.+=(Array[Any](app_key, date, ty, type_value, visitor_count, update, update, visitor_count))

        } else {
          val app_key = r.get(0)
          val type_value = r.get(1)
          val visitor_count = r.get(2)
          val date = r.get(3)
          val ty = r.get(4)
          val update = r.get(5)
          params.+=(Array[Any](app_key, date, ty, type_value, visitor_count, update, update, visitor_count))
        }
      }
      JdbcUtil.doBatch(sqlText, params)
    })
  }
}
