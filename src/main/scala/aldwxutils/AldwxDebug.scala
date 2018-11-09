package aldwxutils

import aldwxconfig.{ConfigurationUtil, Constants}
/**
  * Created by 18510 on 2017/11/1.
  */
object AldwxDebug {
  def main(args: Array[String]): Unit = {
    debug_info("2017-11-01","weilongsheng", false)
  }

  /**
  * <b>author:</b> gcs <br>
  * <b>data:</b> 18-5-4 <br>
  * <b>description:</b><br>
    *   这个类是用来进行debug的 <br>
  * <b>param:</b><br>
  * <b>return:</b><br>
  */
  def debug_info(date_update: String,name:String,etl:Boolean = true): Unit ={
    var hdfs_name = ""
    if (etl == false) {
      hdfs_name = ConfigurationUtil.getProperty("noetlUrl")
    } else {
      hdfs_name = ConfigurationUtil.getProperty("tongji.parquet")
    }
    val mysql_ip = ConfigurationUtil.getProperty(Constants.JDBC_URL).split("//")(1).split(":")(0)
    val mysql_name = ConfigurationUtil.getProperty(Constants.JDBC_URL).split("[/?]")(3)
//      val mysql_ip = aldwxutils.DBConf.url2.split("//")(1).split(":")(0)
//    val mysql_name = aldwxutils.DBConf.url2.split("[/?]")(3)
    println("本类的作者名称: "           + name)
    println("本类的更新新日期: "         + date_update)
    println("本类的包名和类名: "         + this.getClass.getName)
    println("本类所连接的数据源: "       + hdfs_name)
    println("本类所连接的数据库ip地址: "  + mysql_ip)
    println("本类所连接的数据库名字: "    + mysql_name)
  }
}
