package bitMap

//==========================================================
/*
*gcs:
*这个方法是将uu字段存储到HBase当中
*/
import java.text.SimpleDateFormat

import aldwxutils.ArgsTool
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong
import org.apache.curator.retry.{ExponentialBackoffRetry, RetryNTimes}
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.{DataFrame, SparkSession}


object SaveUU2Hbase {

  def main(args: Array[String]): Unit = {

    //==========================================================2
    /*
    *gcs:
    *创建一个sparkSession
    */
    val sparkSession = SparkSession.builder()
      .appName(this.getClass.getName)
//      .master("local[3]")
      .getOrCreate()

    //==========================================================3
    /*
    *gcs:
    *分析args当中传进来的-d -du之类的参数信息
    */
    ArgsTool.analysisArgs(args)
//    val df: DataFrame = sparkSession.read.parquet("D:\\Program\\data\\m\\521small.parquet")
    //==========================================================4
    /*
    *gcs:
    *从ArgsTool.day，通常day是昨天的时间，这里的Array(0)，就是代表读取ArgsTool.day-0 的那一天的数据。即，读取昨天的数据
    */
    val df: DataFrame = ArgsTool.getSpecifyTencentDateDF(args, sparkSession, Array(0))

    //==========================================================5
    /*
    *gcs:
    *这个是待会儿连接HBase数据库的IP地址
    */
    val zkUrl="10.0.100.13,10.0.100.8,10.0.100.14"
//    val tableName="ald_user_id_map"

    //==========================================================6
    /*
    *gcs:
    *要创建的HBase的表的名字
    */
    val tableName="test_user_id_map2"

    //==========================================================7
    /*
    *gcs:
    *将uuid插入到HBase表
    */
    //把hbase中没有的uuid信息插入到hbase
    insertUuid2Hbase(sparkSession,df,ArgsTool.day,tableName,zkUrl)

    sparkSession.close()
  }

  /**<br>gcs:<br>
    *把hbase中没有的uuid信息插入到hbase
    *@param sparkSession 用来创建的sparkSession对象
    *@param df 待会儿用来存储的DataFrame对象
    *@param day 这个day的值就是ArgsTool.day当中的day
    *@param tableName 要把uu存储到哪一个tableName当中
    *@param zkUrl 连接HBase的时候用到的zooKeeper的地址。因为安装HBase的时候要用到ZooKeeper的嘛
    *@param zkPort 连接HBase的时候用到的端口的值
    * */
  def insertUuid2Hbase(sparkSession: SparkSession, df: DataFrame, day: String,tableName: String,zkUrl:String,zkPort:String="2181"): Unit = {

    //==========================================================8
    /*
    *gcs:
    *如果表不存在则创建
    *tableName是用来创建HBase表的表的名字
    *zkUrl 是用来创建HBase表时使用的zooKeeper的地址
    */
    createTableOrDoNothing(tableName, zkUrl)


    //==========================================================9
    /*
    *gcs:
    *在 笔记8 从创建完成这个HBase表之后。
    * 接下来要把df当中的数据存储到HBase当中了
    * 先将uu,ak,ifo字段提取出来之后，再将DateFrame转换为rdd，之后再执行map的操作，将RDD当中所有数据都添加到HBase当中
    */
    val uuidRDD = df.filter("ev='app'").select(
      df("uu"), df("ak"), df("ifo")
    ).rdd
      //==========================================================10
      /*
      *gcs:
      *将df转换为rdd之后的RDD进行map的操作。
      * <key,value>键值对中key=(uu,ak)，value为ifo
      */
      .map(row => ((row.getString(0), row.getString(1)), row.getString(2)))
      //==========================================================11
      /*
      *gcs:
      *之后将转换成功之后的数据进行reduceByKey的操作
      */
      .reduceByKey((ifo1, ifo2) => { //gcs:注意此时的reduceByKey操作中的value是ifo1 或者ifo2字段
        //去重聚合
        if ("true".equals(ifo1) || "true".equals(ifo2)) day else ""  //gcs:这里是什么逻辑啊？？ 为什么要判断ifo1是否为true的操作。之后reduceByKey的操作返回day或者""
      }).map(line => { //gcs:line =>((uu,ak),day) 这个day是这个函数的参数.或者((uu,ak),"") “”是空字符串
      val ifo = if ("true".equals(line._2)) day else ""
      (line._1._1, line._1._2, ifo)  //gcs:这个最终形成的数据的格式是(uu,ak,ifo)
    })
      //==========================================================12
      /*
      *gcs:
      *接下来就该执行往表当中插入数据的操作了
      */
      .mapPartitions(itr => {

      //==========================================================13
      /*
      *gcs:
      *创建一个ArrayBuffer的操作
      */
        val buf = new collection.mutable.ArrayBuffer[(String, String, String,Int)]()

      //==========================================================14
      /*
      *gcs:
      *连接HBase的操作
      */
        //获取hbase连接
        val  connection = getHbaseConn(tableName, zkUrl, zkPort)

      //==========================================================15
      /*
      *gcs:
      *使用hbase获得一个表的连接
      */
        //建立表的连接
        val table = connection.getTable(TableName.valueOf(tableName))


      //==========================================================16
      /*
      *gcs:
      *创建一个Get的list数组。一个Get对象就是表当中的一行数据
      */
        val gets = new java.util.ArrayList[Get]
        val list = itr.toList  //gcs:将所有的itr转换为一个List
      //==========================================================17
      /*
      *gcs:
      *指定往哪一个列族当中写程序。并且指定一个"列限定符"
      */
        //列族
        val cf1 = Bytes.toBytes("user_id")
        //列限定符
        val qf = Bytes.toBytes("sequence_id")

      //==========================================================18
      /*
      *gcs:
      *循环mapPartition当中的每一个Partition当中的数据
      */
        list.foreach {

          //gcs:使用case类，因为list的类型是(String,String,String)类型的无名字的。这样的话使用case类就可以写成有名字的(uu,ak,ifo)字段
          case (uu, ak, ifo) => {

          //==========================================================19
          /*
          *gcs:
          *创建一个行健。行健是由 ak+uu 组建而成
          */
          val row1 = Bytes.toBytes(ak + uu)

          //==========================================================20
          /*
          *gcs:
          *你先创建一个Get对象。你先把要插入HBase表中的每一行数据插入到一个Get对象当中
          * 我可以在这个get对象当中创建列族之类的操作
          */
          val get1 = new Get(row1)  //gcs:将在HBase当中的那一行row1全部都提取出来
            //gcs:往get对象中放入一个列族cf1。待会儿在读的时候会把这个列族当中的数据都读取出来
          get1.addColumn(cf1, qf) //gcs:往这一行当中添加一个列族。cf1是一个列族的名字，qf是这个列族的限定符 。

          //==========================================================21
          /*
          *gcs:
          *之后将这个Get对象放入到这个类型是Get类型的gets的数组当中
          */
          gets.add(get1) //gcs:
        }
        }

      //==========================================================22
      /*
      *gcs:
      *使用table的get方法，从tablename的row行中的列族为cf1="user_id" 笔记20 的数据都提取出来
      */
        //     查看结果集里是否有ak,uu,没有则放入buf
        val results: Array[Result] = table.get(gets) //gcs:这个get的方法是，从HBase的对象中将gets数组中的列都获取出来

        var num=0
      //==========================================================23
      /*
      *gcs:
      *遍历刚刚提取出来的数据。
      */
        for(i<- 0 until results.length){
          if(Bytes.toString(results(i).getRow) == null){
            val uu=list(i)._1
            val ak=list(i)._2
            val ifo=list(i)._3
            num = num+1
            buf.append((uu, ak, ifo,num)) //gcs:将(uu,ak,ifo,num)这个序列存放到Array数组当中
          }
        }

      //==========================================================24
      /*
      *gcs:
      *创建一个时间的格式。将当前的时间进行格式化的操作
      */
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm")
        val update = sdf.format(System.currentTimeMillis())
        //        zk保存增量

        val PATH = "/test/counter"
        val countUrl="10.0.100.13:2181,10.0.100.8:2181,10.0.100.14:2181"
        val client = CuratorFrameworkFactory.newClient(countUrl, new ExponentialBackoffRetry(1000, 30))
        client.start()
        //计算增量值
      //==========================================================25
      /*
      *gcs:
      *
      */
        val count = new DistributedAtomicLong(client, PATH, new RetryNTimes(10, 10))
        val add = count.add(buf.length.toLong)
        val preValue = add.preValue()

        val rs = buf.map {
          case (user_id, ak, ifo, num) => {

            //==========================================================26
            /*
            *gcs:
            *这是执行的往表中插入数据的操作吗???
            */
            val sequence_id = num + preValue
            val put = new Put(Bytes.toBytes(s"$ak$user_id"))   //gcs:往行健为ak+user_id行健的那一行插入数据


            //==========================================================27
            /*
            *gcs:
            *往行健为ak+user_id的行的 ak 的列族的ak列中插入 Bytes.toBytes(ak)
            * 难道说，"列限定符"就是指列族中的列名吗？？？
            */
            put.addColumn(Bytes.toBytes("ak"), Bytes.toBytes("ak"), Bytes.toBytes(ak))

            //==========================================================28
            /*
            *gcs:
            *往行健为ak+user_id的行的 user_id 的列族的sequence_id 列中插入 Bytes.toBytes(ak)
            */
            put.addColumn(Bytes.toBytes("user_id"), Bytes.toBytes("user_id"), Bytes.toBytes(user_id))
            put.addColumn(Bytes.toBytes("user_id"), Bytes.toBytes("sequence_id"), Bytes.toBytes(sequence_id.toInt))
            put.addColumn(Bytes.toBytes("ifo"), Bytes.toBytes("day"), Bytes.toBytes(ifo))
            put.addColumn(Bytes.toBytes("time"), Bytes.toBytes("update"), Bytes.toBytes(update))
            //            myTable.put(put)
            (new ImmutableBytesWritable, put)
          }
        }

        //关闭连接
        client.close()
        connection.close()

      //==========================================================29
      /*
      *gcs:
      *这里是什么意思，为什么要把刚刚创建的Put对象返还出去
      */
        rs.iterator
      })


    /**
      * .map { case ((user_id, ak, ifo), index) => {
        val sequence_id = count + index//
        val put = new Put(Bytes.toBytes(s"$ak$user_id"))
        put.addColumn(Bytes.toBytes("ak"), Bytes.toBytes("ak"), Bytes.toBytes(ak))
        put.addColumn(Bytes.toBytes("user_id"), Bytes.toBytes("user_id"), Bytes.toBytes(user_id))
        put.addColumn(Bytes.toBytes("user_id"), Bytes.toBytes("sequence_id"), Bytes.toBytes(sequence_id.toInt))
        put.addColumn(Bytes.toBytes("ifo"), Bytes.toBytes("day"), Bytes.toBytes(ifo))
        put.addColumn(Bytes.toBytes("time"), Bytes.toBytes("update"), Bytes.toBytes(update))
        (new ImmutableBytesWritable, put)
      }
      */

      //==========================================================30
      /*
      *gcs:
      *连接hbase，将数据
      */
    val jobConf = new JobConf()
    jobConf.set("hbase.zookeeper.quorum", zkUrl)
    //    jobConf.set("zookeeper.znode.parent", "/hbase")
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    uuidRDD.saveAsHadoopDataset(jobConf) //gcs:将产生的DateSet存储到HBase当中

  }


  //获取hbase连接
  /**<br>gcs:<br>
    * 获取一个hbase的连接
    * @param tableName 这个tableName在这个函数当中灭有使用。这应该是指代要使用哪一个HBase函数
    * @param zkUrl 是用来连接HBase的zooKeeper的地址
    * @param zkPort 是用来连接HBase的端口号
    * */
  private def getHbaseConn(tableName: String, zkUrl: String, zkPort: String) = {
    val configuration = HBaseConfiguration.create
    //设置zooKeeper集群地址
    configuration.set("hbase.zookeeper.quorum", zkUrl)
    //设置zookeeper连接端口，默认2181
    configuration.set("hbase.zookeeper.property.clientPort", zkPort)
    val connection = ConnectionFactory.createConnection(configuration)
    connection
  }

  // 如果表不存在则创建表
  /**<br>gcs:<br>
    * 如果表不存在则创建表。如果要创建的表之前已经创建过了，此时就不会再执行任何的操作了 <br>
    *@param tableName 要创建的表的名字
    *@param zkUrl 这个URl就是HBase安装的URL。说白了就是说明HBase安装在了哪些机器上面
    * */
  private def createTableOrDoNothing(tableName: String, zkUrl: String) = {


    //==========================================================1
    val conf = HBaseConfiguration.create() //gcs:创建一个HBase的配置对象

    //==========================================================2
    //设置HBase的zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    conf.set("hbase.zookeeper.quorum", zkUrl)
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    //==========================================================3
    /*
    *gcs:
    *TableInputFormat 的作用是将一个HBase的表转换为Map/Reduce 函数可以进行操作的表
    * 这个配置的作用是将这个tableName的HBase表转换为Map/Reduce可以进行操作的表
    */
    conf.set(TableInputFormat.INPUT_TABLE, tableName)


    //==========================================================4
    /*
    *gcs:
    *使用conf 配置对象创建一个HBase的HBaseAdmin的对象
    */
    // 如果表不存在则创建表
    val admin = new HBaseAdmin(conf)
    //==========================================================5
    /*
    *gcs:
    *如果在Hbase表中我们要创建的表是不存在的。此时就会创建这个名字为tableName的表，之后再创建4个列族
    */
    if (!admin.isTableAvailable(tableName)) { //gcs:如果表不存在的话，此时就会执行if语句当中的话

      //==========================================================6
      /*
      *gcs:
      *创建一个HBase表。并且为这个表创建4个列族。
      *这4个列族的名字分别为user_id，
      */
      val tableDesc = new HTableDescriptor(TableName.valueOf(tableName))
      tableDesc.addFamily(new HColumnDescriptor("user_id"))  //gcs:为这个表创建一个列族。这个列族的名字为user_id
      tableDesc.addFamily(new HColumnDescriptor("ak"))
      tableDesc.addFamily(new HColumnDescriptor("ifo"))
      tableDesc.addFamily(new HColumnDescriptor("time"))
      admin.createTable(tableDesc)  //gcs:在创建完这些列族之后，再时间创建这个表的操作
    }
  }


}
