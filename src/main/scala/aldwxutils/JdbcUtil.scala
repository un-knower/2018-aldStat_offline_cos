//==========================================================f2
package aldwxutils

import java.sql.{Connection, DriverManager, SQLException}
import java.util

import aldwxconfig.{ConfigurationUtil, Constants}
import com.mysql.jdbc.MysqlDataTruncation
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * 数据库配置, 批量入库
  */
object JdbcUtil {
  private var pools: util.LinkedList[Connection] = _ //连接池
  private val driver = ConfigurationUtil.getProperty(Constants.JDBC_DRIVER)
  private val url = ConfigurationUtil.getProperty(Constants.JDBC_URL)
  private val username = ConfigurationUtil.getProperty(Constants.JDBC_USER)
  private val password = ConfigurationUtil.getProperty(Constants.JDBC_PWD)

  /**
    * 加载驱动
    */
  Class.forName(driver)

  /**
    * 从数据库连接池中获取连接对象
    *
    * @return
    */

  def getConn(): Connection = {
    synchronized({
      try {
        if (pools == null) {
          pools = new util.LinkedList[Connection]()
          //连接池大小=cup数*2+磁盘数
          for (_ <- 0 until 17) {
            val connection = DriverManager.getConnection(url, username, password)
            pools.push(connection)
          }
        }

      } catch {
        case e: SQLException => e.printStackTrace()
      }
      /*这里如果不加判断，可能会发生取出null值的情况，
      但是实际任务中只出现过一两次，故先不做判断，继续观察任务情况*/
      //      if (pools.size() == 0) {
      //        do {
      //          println("线程池空了，睡3s")
      //          Thread.sleep(3000)
      //        } while (pools.size() == 0)
      //      }
      return pools.poll()
    })
  }

  /**
    * 当连接对象用完后，需要调用这个方法归还连接对象到连接池中
    *
    * @param connection
    */
  def returnConnection(connection: Connection): Unit = {
    pools.push(connection)
  }



  /**
  * <b>author:</b> gcs <br>
  * <b>data:</b> 18-5-6 <br>
  * <b>description:</b><br>
    *   数据库批量入库 <br>
    *     这个批量操作的含义是，SQL语句是一样的，只是需要一次性的往这条SQL语句中插入n条数据。
    *     批量插入数据的优点是利用一个conn可以一次性的插入n条记录。如果不采用批量插入的方式，我们在插入这n条数据时，要建立n次conn。使用批量插入可以减少n-1次的conn的建立的时间。可以很大可能地节省时间。<br>
  * <b>param:</b><br>
    *   sqlText: String ;往目标表中插入数据的sql语句 <br>
    *     params: ArrayBuffer[Array[Any]] ；给sqlText 提供具体的插入的数据的值 <br>
  * <b>return:</b><br>
    *   null <br>
  */
  def doBatch(sqlText: String, params: ArrayBuffer[Array[Any]]): Unit = {

    var conn = getConn()  //gcs:从线程池中取得一个连接
    if (conn == null) {
      do {
        println("线程池空了，睡3s")
        Thread.sleep(3000)
        conn = getConn() //gcs:隔3000ms之后使用递归函数，再次进行MySql数据库的连接
      } while (conn == null)
    }


    if (params.nonEmpty) { // 判断sql 数据数组是否为空 //gcs:这个params数组的作用是什么呢？？？
      val pstat = conn.prepareStatement(sqlText) //gcs:预编译 sql 语句
      pstat.clearBatch() // 清除批处理
      var count = 0
      for (param <- params) {
        for (index <- param.indices) {
          pstat.setObject(index + 1, param(index)) // index + 1 是指 sql 语句的第几个参数, param(index)是要设置的值
        }
        pstat.addBatch() // 添加到批次中
        count += 1
        if (count == 1000) { //gcs:当要插入的数据为达到1000之后，就会批量地进行操作
          try {
            pstat.executeBatch() // 批量执行
          } catch {
            case e: SQLException =>
              println(s"插入mysql异常,状态码：${e.getSQLState}，错误码：${e.getErrorCode},message:${e.getMessage},错误的数据${param.mkString(",")}")
            case e: MysqlDataTruncation =>
              println(s"插入异常==MysqlDataTruncation ：${e.getMessage}，异常数据：${param.mkString(",")}")
            case e: Exception =>
              println(s"插入异常：${e.getMessage}，异常数据：${param.mkString(",")}")
          }
          count = 0  //gcs:批量操作完成之后，将count重新设置为0
        }
      }
      try{
        pstat.executeBatch() // 批量执行
      }catch {
        case e: SQLException =>
          println(s"插入mysql异常,状态码：${e.getSQLState}，错误码：${e.getErrorCode},message:${e.getMessage}")
        case e: MysqlDataTruncation =>
          println(s"插入异常==MysqlDataTruncation ：${e.getMessage}")
        case e: Exception =>
          println(s"插入异常：${e.getMessage}")
      }

      params.clear() // 清除 params 数组
    }else{
      println("传过来的字段参数params是空的")
    }
    returnConnection(conn) //gcs:将MYSql的连接归还MySql的线程池
  }

  //批量并且失败回滚
  /* def doBatch(sqlText: String, params: ArrayBuffer[Array[Any]]): Unit = {
     var conn = getConn()
     //不判断的话可能取到null值，因为是并行去list中取的
     while (conn == null) {
       println("doBatch中的conn为空了,等待3秒在重试..")
       Thread.sleep(3000)
       conn = getConn()
     }
     val pstat = conn.prepareStatement(sqlText) // 预编译 sql 语句
     try {
       conn.setAutoCommit(false)
       if (params.nonEmpty) { // 判断sql 数据数组是否为空
         var count = 0
         pstat.clearBatch() // 清除批处理
         for (param <- params) {
           for (index <- param.indices) {
             // index + 1 是指 sql 语句的第几个参数, param(index)是要设置的值
             pstat.setObject(index + 1, param(index))
           }
           pstat.addBatch() // 添加到批次中
           count += 1
           if (count == 1000) {
             pstat.executeBatch() // 批量执行
             conn.commit()
             count = 0
           }
         }
         pstat.executeBatch() // 批量执行
         conn.commit()
         println(s"提交后conn为 $conn")
         params.clear() // 清除 params 数组
       }
     } catch {
       case e: SQLException =>
         println(s"插入mysqls异常,状态码：${e.getSQLState}，错误码：${e.getErrorCode},message:${e.getMessage}")
         println(e.printStackTrace())
         try {
           println("插入异常，sql回滚")
           println(s"rollback前conn为 $conn")
           conn.rollback()
           println(s"rollback后conn为 $conn")
         } catch {
           case er: SQLException =>
             println(er.printStackTrace())
         }
     } finally {
       close(pstat, conn)
     }


   }*/

  //==========================================================1
  /*gcs:
  *转自,f1,1
  */
  /**
  * <b>author:</b> wty <br>
  * <b>data:</b> 18-5-3 <br>
  * <b>description:</b><br>
    *   将一个MySql数据库当中的所有的内容都读取出来 <br>
  * <b>param:</b><br>
    *   sparkSession: SparkSession 可序列化。利用这个SparkSession将MySql当中的一个table当中的数据都读取出来 <br>
    *     table: String 它是MySql的一个数据库。这个函数会把table当中的所有的数据都读取出来 <br>
  * <b>return:</b><br>
    *   DataFrame 返回table的所有的数据 <br>
  */
  def readFromMysql(sparkSession: SparkSession, table: String): DataFrame = {

    val url = ConfigurationUtil.getProperty(Constants.JDBC_URL) //gcs:得到MySql的地址
    val user = ConfigurationUtil.getProperty(Constants.JDBC_USER) //gcs:得到MySql的user的name
    val password = ConfigurationUtil.getProperty(Constants.JDBC_PWD) //gcs:得到MySql的密码
    val driver = ConfigurationUtil.getProperty(Constants.JDBC_DRIVER) //gcs:得到MySql的Driver
    // 从 mysql 中读取数据
    val jdbcDF = sparkSession.read
      .format("jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("dbtable", table)
      .option("user", user)
      .option("password", password)
      .load()
    jdbcDF
  }
}
