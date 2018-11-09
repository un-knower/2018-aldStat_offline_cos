package aldwxstat

import org.apache.spark.Partitioner

/**
  * Created by wangtaiyang on 2018/4/22. 
  */
class UserStayDailyPartitioner extends Partitioner {
  override def numPartitions: Int = 269


  override def getPartition(key: Any): Int = {
    UserStayDailyPartitioner.autoIncrement()
  }

}

object UserStayDailyPartitioner {
  val records: Long = UserStayDaily.records
  val everyRegions: Long = records / 270
  var regionRecords = 0L
  var partition = 0

  def autoIncrement(): Int = {
    synchronized({
      if (regionRecords < everyRegions)
        regionRecords += 1
      else {
        partition += 1
        regionRecords = 0L
      }
    })
    partition
  }
}
