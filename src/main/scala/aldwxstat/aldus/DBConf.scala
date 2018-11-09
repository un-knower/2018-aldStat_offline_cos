package aldwxstat.aldus

import java.sql.{Connection, DriverManager}

import org.apache.spark.sql.{DataFrame, SparkSession}


/**
* <b>author:</b> zhangyanpeng <br>
* <b>data:</b> 2017/7/12 <br>
* <b>description:</b><br>
  *   连接数据库的一些配置信息 <br>
* <b>param:</b><br>
* <b>return:</b><br>
*/
object DBConf {

   val driver   = "com.mysql.jdbc.Driver"
   val hdfsUrl = "hdfs://10.0.0.212:9000/ald_log_parquet"
   val noetlUrl = "hdfs://10.0.0.212:9000/ald_jsonlogs" // ETL 之前的 json 数据源
   val hdfsPath = "hdfs://10.0.0.212:9000"
   val errorPath = "hdfs://10.0.0.212:9000/ald_err_parquet"
   val hdfsEtl   = "hdfs://10.0.0.212:9000/ald_log_etl/"
   val cosUrl = "cosn://aldwxlogbackup/log_parquet/"

   val url= "jdbc:mysql://10.0.0.179:3306/ald_xinen?useUnicode=true&characterEncoding=UTF-8&useUnicode=true&characterEncoding=utf-8&autoReconnect=true&failOverReadOnly=false"
   val user="cdb_outerroot"
   val password="%06Ac9c@317Hb&"

  def read_from_mysql(sparkSession: SparkSession, table: String): DataFrame = {
    // 从 mysql 中读取数据
    val jdbcDF = sparkSession.read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", s"${url}")
      .option("dbtable", table)
      .option("user", s"${user}")
      .option("password", s"${password}")
      .load()
    jdbcDF
  }

  def getConn: Connection = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection(url, "aldwx", "wxAld2016__$#")
  }
}
