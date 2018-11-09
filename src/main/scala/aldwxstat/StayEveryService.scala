package aldwxstat

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * Created by cuizhangxiu on 2018/1/9.
  */
class StayEveryService extends StayService {

  /**
    * 求留存率
    * 生成一个综合数据的临时表
    * @param spark 与main方法公用同一个SparkSession
    */
  override def dayNumDayStay(spark: SparkSession, everyDayData: DataFrame): Unit = {
    //新用户人数dailyPeopleDF
    val dailyPeopleDF = everyDayData.filter("ifo='true'")
      .select(
        everyDayData("ak").alias("base_app_key"),
        from_unixtime((everyDayData("st")/1000),"yyyy-MM-dd").alias("base_day"),
        everyDayData("uu").alias("base_uuid"),
        everyDayData("ifo").alias("base_is_first_come")
      )
    dailyPeopleDF.createOrReplaceTempView("dailyPeopleDF")

    //统计活跃用户 （ifo =‘’）dayAfterDF
    val dayAfterDF = everyDayData.select(
      everyDayData("ak").alias("app_key"),
      from_unixtime((everyDayData("st")/1000),"yyyy-MM-dd").alias("day_after"),
      everyDayData("uu").alias("day_after_uuid"),
      everyDayData("ifo").alias("day_after_is_first_come")
    )
    dayAfterDF.createOrReplaceTempView("dayAfterDF")

    //将两个表链接起来，预备所搜需要的行PrepareDecareDF1
    val PrepareDecareDF1 = spark.sql("select * from dailyPeopleDF join dayAfterDF")

    //获得两个日期之间的间隔天数字段
    val addDiffDF = PrepareDecareDF1.select(
      PrepareDecareDF1("base_app_key"),           //新增ak
      PrepareDecareDF1("base_day"),               //新增day
      PrepareDecareDF1("base_uuid"),              //新增用户uu
      PrepareDecareDF1("base_is_first_come"),     //ifo   true
      PrepareDecareDF1("day_after"),              //天数
      PrepareDecareDF1("day_after_uuid"),         //活跃用户uu
      datediff(PrepareDecareDF1("day_after"), PrepareDecareDF1("base_day")).alias("diff") //返回两个日期之间的间隔
    ) .filter("base_uuid==day_after_uuid")
      .distinct()

    //将新用户留存数计算出来，并且把空值数据用0填充
    //base_app_key,base_day, diff , is_first_come , new_people_left
    val newUStayDF = addDiffDF.select(
      addDiffDF("base_app_key"),
      addDiffDF("base_day"),
      addDiffDF("diff"),
      when(addDiffDF("base_is_first_come").isNull, 0).otherwise(1).alias("is_first_come")
    ).groupBy("base_app_key", "base_day", "diff")
      //分组聚合 以"base_app_key", "base_day", "diff" 分组 求出is_first_come的总和 改名为 new_people_left
      .agg(sum("is_first_come") as "new_people_left")
      .na.fill(0)
      //.withColumn("new_people_left", lit(0)) //拼接到后面的一列
      .filter(column("diff")===30 || column("diff")===14 || column("diff")===7 || column("diff")===6 || column("diff")===5 || column("diff")===4 || column("diff")===3 || column("diff")===2 || column("diff")===1 || column("diff")===0) //过滤


    //-------------------------------------
    //统计时间内的 活跃用户 active_app_key  active_day  active_uuid
    val activePeopleDF =everyDayData.select(
      everyDayData("ak").alias("active_app_key"),
      from_unixtime((everyDayData("st")/1000),"yyyy-MM-dd").alias("active_day"),
      everyDayData("uu").alias("active_uuid")
      //df("ifo").alias("active_is_first_come")
    )
    //将DF弄成一个临时表
    activePeopleDF.createOrReplaceTempView("active_people_df")

    //将两个表链接起来，预备所搜需要的行
    val activeResultDF = spark.sql("select * from active_people_df join dayAfterDF")

    //active_app_key  active_day  active_uuid  active_uuid   day_after  day_after_uuid  diff
    //获得以上字段
    val activeUserDF = activeResultDF.select(
      activeResultDF("active_app_key"),           //活跃用户ak
      activeResultDF("active_day"),               //活跃用户day
      activeResultDF("active_uuid"),              //活跃用户uu
      //active_result_df("active_is_first_come"),     //ifo
      activeResultDF("day_after"),              //天数
      activeResultDF("day_after_uuid"),         //活跃用户uu
      datediff(activeResultDF("day_after"), activeResultDF("active_day")).alias("diff") //返回两个日期之间的间隔
    ) .filter("active_uuid==day_after_uuid")
      .distinct()

    //active_app_key active_day diff  day_after_uuid  people_left
    //获得用户留存数, 并把字段为空 替换成0
    val userStayDF = activeUserDF.select(
      activeUserDF("active_app_key"),
      activeUserDF("active_day"),
      activeUserDF("diff"),
      activeUserDF("day_after_uuid")
      //when(active_re_df("active_is_first_come").isNull, 1).otherwise(1).alias("active_come")
    ).groupBy("active_app_key", "active_day", "diff")
      //不分组聚合
      .agg(countDistinct("day_after_uuid")  as "people_left")
      .na.fill(0)
      //.withColumn("new_people_left", lit(0)) //拼接到后面的一列
      .filter(column("diff")===30 || column("diff")===14 || column("diff")===7 || column("diff")===6 || column("diff")===5 || column("diff")===4 || column("diff")===3 || column("diff")===2 || column("diff")===1 || column("diff")===0) //过滤

    //将两个DF数据进行join 将对获得mysql字段有用的数据字段都合成一个新的DF
    //ak  day time pleft npleft
    val perpareDataDF = userStayDF.join(newUStayDF
      , userStayDF("active_app_key") === newUStayDF("base_app_key") &&
      userStayDF("active_day") === newUStayDF("base_day") &&
      userStayDF("diff") === newUStayDF("diff")
    ).select(
      userStayDF("active_app_key") alias("ak"),
      userStayDF("active_day") alias("day"),
      userStayDF("diff") alias("time"),
      userStayDF("people_left") alias("pleft"),
      newUStayDF("new_people_left") alias("npleft")
    )

    //将时间间隔为0天的数据过滤出来
    val timeIsZeroDF =perpareDataDF.filter(perpareDataDF("time")===0)
      .select(
        perpareDataDF("ak") alias("r_ak"),
        perpareDataDF("day") alias("r_day"),
        perpareDataDF("npleft") alias("r_npleft"),
        perpareDataDF("pleft") alias("r_pleft")
      )

    //将两个DF进行join  获得预存表 table
    timeIsZeroDF.join(perpareDataDF,perpareDataDF("ak")===timeIsZeroDF("r_ak") && perpareDataDF("day") === timeIsZeroDF("r_day"))
      .select(
        perpareDataDF("ak"),
        perpareDataDF("day"),
        perpareDataDF("time"),
        perpareDataDF("pleft"),
        perpareDataDF("npleft"),
        timeIsZeroDF("r_npleft"),
        timeIsZeroDF("r_pleft")
      ).createTempView("table")

  }
}
