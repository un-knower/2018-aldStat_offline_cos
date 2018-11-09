package bitMap

import aldwxutils.ArgsTool
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Result}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{DataFrame, SparkSession}

object SaveBitArr {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName(this.getClass.getName)
      //      .master("local[3]")
      .getOrCreate()

    ArgsTool.analysisArgs(args)
    //    val df: DataFrame = sparkSession.read.parquet("D:\\Program\\data\\m\\521small.parquet")
    val df: DataFrame = ArgsTool.getSpecifyTencentDateDF(args, sparkSession, Array(0))


    //    val df: DataFrame = ArgsTool.getLogs(args, sparkSession, )

    val zkUrl="10.0.100.13,10.0.100.8,10.0.100.14"
    //    val tableName="ald_user_id_map"
    val tableName="test_user_id_map1"
    val savePath="hdfs://10.0.0.58:4007/tmp/test/bitmap/arr3"
    //把hbase中没有的uuid信息插入到hbase
    saveBitArr(sparkSession,df,ArgsTool.day,tableName,zkUrl,savePath)

    sparkSession.close()
  }


//  def saveBitArr2(sparkSession: SparkSession, df: DataFrame, savePath: String) = {
  /**
  * <b>author:</b> gcs <br>
  * <b>data:</b> 18-7-4 <br>
  * <b>description:</b><br>
    *   这个方法当中有涉及Hbase的使用的方法 <br>
  * <b>param:</b><br>
    *
  * <b>return:</b><br>
  */
  def saveBitArr(sparkSession: SparkSession, df: DataFrame, day: String,tableName: String,zkUrl:String,savePath: String, zkPort:String="2181") = {

    val df2 = df.filter("ev='app'").select(
      df("uu"), df("ak")
    ).rdd.map(row => (row.getString(0), row.getString(1))).distinct()//(uu,ak)
      .mapPartitions(itr => {

        val buf = new collection.mutable.ArrayBuffer[Int]()

        val configuration = HBaseConfiguration.create
        //设置zooKeeper集群地址
        configuration.set("hbase.zookeeper.quorum", zkUrl)
        //设置zookeeper连接端口，默认2181
        configuration.set("hbase.zookeeper.property.clientPort", zkPort)
        val connection = ConnectionFactory.createConnection(configuration)
        //建立表的连接
        val table = connection.getTable(TableName.valueOf(tableName))
        val gets = new java.util.ArrayList[Get]
        val list = itr.toList

      //==========================================================1
      /*
      *gcs:
      *设定一个列族
      */
        //列族
        val cf1 = Bytes.toBytes("user_id")
      //==========================================================2
      /*
      *gcs:
      *创建一个列族
      */
        //列限定符。列限定符，说白了就是列族当中的"列"
        val qf = Bytes.toBytes("sequence_id")
        list.foreach { case (uu, ak) => {
          //行键
          val row1 = Bytes.toBytes(ak + uu)
          val get1 = new Get(row1)
          get1.addColumn(cf1, qf)
          gets.add(get1)
         }

        }

        //     查看结果集里是否有ak,uu,有则把sequence_id放入buf
        val results: Array[Result] = table.get(gets)


      //==========================================================3
      /*
      *gcs:
      *遍历数组，
      */
        for(i<- 0 until results.length){

          val sequence_id = Bytes.toInt(results(i).getValue(cf1,qf))
          buf.append(sequence_id)
        }
      table.close()
      connection.close()
        buf.iterator
      })
      .map(x=>( x>>5,1<<(x & 0X1F)))//转成arr
      .reduceByKey { case (x, y) => x | y }
    val map = df2.collect()
    val max = map.map(_._1).max
    val map1 = new Array[Int](max + 1)
    for (e <- map) map1(e._1) = e._2
    sparkSession.sparkContext.parallelize(map1, 1)
      .saveAsTextFile(savePath+day)
  }
}
