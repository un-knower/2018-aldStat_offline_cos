package aldwxstat

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by cuizhangxiu on 2018/1/9.
  */
trait StayService {

  /**
    * 求留存率
    * 生成一个综合数据的临时表
    * @param spark 与main方法公用同一个SparkSession
    */
  def dayNumDayStay(spark : SparkSession,everyDayData:DataFrame)

}
