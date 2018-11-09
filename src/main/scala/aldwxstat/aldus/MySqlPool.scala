package aldwxstat.aldus

import java.sql.{Connection, DriverManager}
import java.util



/**
* <b>author:</b> zhangyanpeng <br>
* <b>data:</b> 2017/7/21 <br>
* <b>description:</b><br>
  *   创建线程池
* <b>param:</b><br>
* <b>return:</b><br>
*/
object MySqlPool {
  private val max = 100                                    //连接池连接总数
  private val connectionNum = 10                           //每次产生连接数
  private var conNum = 0                                   //当前连接池已产生的连接数
  private val pool = new util.LinkedList[Connection]()     //连接池
  private val driver = DBConf.driver
  private val url = DBConf.url  //gcs:这里指连接用户的MySql的配置的信息
  private val username =DBConf.user  //gcs:连接的数据库的用户的名字
  private val password = DBConf.password //gcs:连接的数据库的用户的密码


  def getJdbcConn() : Connection = {

    //同步代码块  同步锁
    AnyRef.synchronized({
      if(pool.isEmpty){

        //==========================================================1
        /*gcs:
        *如果MySql的线程池是空的，此时就初始化线程池
        */
        //加载驱动
        /*getJdbcConn()*/
        for(i <- 1 to connectionNum){
          //连接数据库
          val conn = DriverManager.getConnection(url,username,password) //gcs:连接MySql的指定的数据库
          pool.push(conn)
          conNum +=  1
        }
      }

      //gcs:如果MySql线程池当中已经有数据了。此时就返回线程池中的一个MySql的连接
      pool.poll()
    })
  }

  /**
  * <b>author:</b> gcs <br>
  * <b>data:</b> 18-5-4 <br>
  * <b>description:</b><br>
    *   使用完一个MySql的驱动连接之后，再把这个MySql的连接释放给连接池
  * <b>param:</b><br>
  * <b>return:</b><br>
  */
  def releaseConn(conn:Connection): Unit ={
    pool.push(conn)
  }

  /**
  * <b>author:</b> gcs <br>
  * <b>data:</b> 18-5-4 <br>
  * <b>description:</b><br>
    *   加载驱动 。如果线程池中没有驱动的话就会让这个程序一直处于等待程序执行的过程中<br>
  * <b>param:</b><br>
  * <b>return:</b><br>
  */
  private def preGetConn() : Unit = {

    //控制加载
    if(conNum < max && !pool.isEmpty){
      println("Jdbc Pool has no connection now, please wait a moments!")
      Thread.sleep(2000)
      preGetConn()  //gcs:递归函数，每隔2000ms再去执行这个函数
    }else{
      Class.forName("com.mysql.jdbc.Driver")
    }
  }
}
