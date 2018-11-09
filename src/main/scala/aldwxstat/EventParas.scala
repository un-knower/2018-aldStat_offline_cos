//==========================================================f4
/*gcs:
事件分析-事件参数列表
将用户创建的事件的具体的信息，事件的名字和一些属性信息从parquet数据库当中提取出来，之后写到数据库当中
*/

package aldwxstat

import java.security.MessageDigest

import aldwxconfig.ConfigurationUtil
import aldwxutils.{ArgsTool, EmojiFilter, JdbcUtil}
import com.alibaba.fastjson.JSON
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by gaoxiang on 2018/1/11.
  */
object EventParas {
  def main(args: Array[String]): Unit = {

    //==========================================================1
    Logger.getLogger("org").setLevel(Level.WARN)  //gcs:设定Log日志的级别为WARN


    //==========================================================2
    //gcs:创建sparkSession对象
    val ss = SparkSession.builder() //gcs:创建一个SparkSession实例对象
      .appName(this.getClass.getName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //gcs:序列化机制为 KryoSerializer 序列化
      .config("spark.kryo.registrator", "aldwxutils.Registers")
      .getOrCreate()
    //    val monitor=new Monitor()
    //    monitor.start()
    //获取昨天时间
    //val yesterday = aldwxutils.TimeUtil.processArgs(args)
    //val yesterday = aldwxutils.TimeUtil.processArgs(args)
    allEventParas(ss, args)
    ss.close()
  }

  /**
    * {"wsr":"{\"reLaunch\":false,\"relaunch\":false,\"scene\":1042,\"query\":{},\"path\":\"pages\/index\/index\"}",
    * "st":"1523191489138","ev":"event","province":"广东","ak":"d3e9823ad8603bb204337c578a965367","v":"5.4.1","wh":"478",
    * "scene":1042,"wv":"6.6.5","country":"中国","lua_wechat_version":"6.6.5.1280(0x26060536)","query":{},"ww":"360",
    * "client_ip":"219.133.65.29","at":"15231910570663949131","city":"深圳","rq_c":"27","relaunch":false,"server_time":1523191490.091,
    * "lang":"zh_CN","tp":"注册成功"===事件id：ev_id,"pr":"3","uu":"15231910181295786351","path":"pages\/index\/index","lat":"undefined","spd":"undefined",
    * "ct":"{\"ald_link_key\":\"4c7030fa684dc931\"}","pm":"BLN-AL40","nt":"wifi","lng":"undefined","ct_ald_link_key":"4c7030fa684dc931"}
    * ct: 事件参数，参数名：ald_link_key，参数值：4c7030fa684dc931
    *
    *
    *
    *
    */
  /**
  * <b>author:</b> gcs <br>
  * <b>data:</b> 18-5-14 <br>
  * <b>description:</b><br>
    *   {"wsr":"{\"reLaunch\":false,\"relaunch\":false,\"scene\":1042,\"query\":{},\"path\":\"pages\/index\/index\"}",
    * "st":"1523191489138","ev":"event","province":"广东","ak":"d3e9823ad8603bb204337c578a965367","v":"5.4.1","wh":"478",
    * "scene":1042,"wv":"6.6.5","country":"中国","lua_wechat_version":"6.6.5.1280(0x26060536)","query":{},"ww":"360",
    * "client_ip":"219.133.65.29","at":"15231910570663949131","city":"深圳","rq_c":"27","relaunch":false,"server_time":1523191490.091,
    * "lang":"zh_CN","tp":"注册成功"===事件id：ev_id,"pr":"3","uu":"15231910181295786351","path":"pages\/index\/index","lat":"undefined","spd":"undefined",
    * "ct":"{\"ald_link_key\":\"4c7030fa684dc931\"}","pm":"BLN-AL40","nt":"wifi","lng":"undefined","ct_ald_link_key":"4c7030fa684dc931"}
    * ct: 事件参数，参数名：ald_link_key，参数值：4c7030fa684dc931
  * <b>param:</b><br>
  * <b>return:</b><br>
  */
  def allEventParas(ss: SparkSession, args: Array[String]): Unit = {

    //==========================================================2-2
    //gcs:分析数据的参数
    ArgsTool.analysisArgs(args) //gcs:分析 用户在程序中的 -d -ak -vak的参数提取出来


    val du = ArgsTool.du //gcs:这个du是什么意思啊? 执行多少天的数据。-du是值分析过去多少天的数据
    val yesterday = ArgsTool.day  //gcs:将yesterday 赋值成 ArgsTool.day

    //    val rdd = Aldstat_args_tool.analyze_args(args,ss,DBConf.hdfsUrl).filter("ev='event'and tp !='ald_error_message' and tp !='ald_reachbottom' and tp !='ald_share_click' and tp !='ald_share_status' and tp !='ald_share_user' and tp !='ald_share_chain' and tp !='ald_pulldownrefresh' and tp !='js_err' and ak!='d6be9d076493c2c3f51b0dbf1ac5cab0'").cache()
//    val rdd = ArgsTool.getLogs(args, ss, ConfigurationUtil.getProperty("tongji.parquet")).filter("ev='event'and " +
//      "tp !='ald_error_message' and tp !='ald_reachbottom' and tp !='ald_share_click' and tp !='ald_share_status' and " +
//      "tp !='ald_share_user' and tp !='ald_share_chain' and tp !='ald_pulldownrefresh' and tp !='js_err' and " +
//      "ak!='d6be9d076493c2c3f51b0dbf1ac5cab0'").cache()

    //==========================================================3
    /*gcs:
    *读取到7天或者30天的数据
    */
    val rdd = ArgsTool.getTencentSevenOrThirty(ss,du) //gcs:从 ArgsTool.day 开始取出du天的数据

      //gcs:读取出数据之后再进行筛选.将我们给用户默认创建的tp的值都帅选出来
      .filter("ev='event'and " +
      "tp !='ald_error_message' and tp !='ald_reachbottom' and tp !='ald_share_click' and tp !='ald_share_status' and " +
      "tp !='ald_share_user' and tp !='ald_share_chain' and tp !='ald_pulldownrefresh' and tp !='js_err' and " +
      "ak!='d6be9d076493c2c3f51b0dbf1ac5cab0'").cache()


    rdd.printSchema() //gcs:打印出表头的列表



    val map: mutable.Map[String, String] = scala.collection.mutable.Map()


    val jSONRDD = rdd.toJSON.rdd.map(f = line => {
      map.clear() //gcs:清除map数组

      val jsonLine = JSON.parseObject(JSON.parseObject(line.toString).toString) //gcs:使用阿里的json数据的转换的方法将ali的json数据
      var str: String = null


      //==========================================================n2-1
      /*
      *gcs:
      *判断方法
      * ak,tp,ct 不为null
      */
      if (jsonLine.get("ak") != null && jsonLine.get("tp") != null) { //gcs:如果ak和 tp 存在的话

        val ak = jsonLine.get("ak").toString //gcs:取出用户的app_key
        val tp = jsonLine.get("tp").toString //gcs:取出用户的event_key

        var a = ""
        var b = ""

        if (jsonLine.get("ct") != null) { //gcs:如果ct不为null的话，执行这个操作

          val ct = jsonLine.get("ct").toString //gcs:将ct字段转为string类型的数据

          if (ct.substring(0, 1).equals("{")) { //gcs:如果ct的0-1字段包括 { 的话

            if (!ct.toString.equals("{}")) { //gcs:如果ct不等于 {}。ct字段中存储着一个事件的所有的事件参数。如果ct字段为{}，就说明这个事件中没有事件参数

              val ctString = ct.toString.substring(1, ct.toString.length - 1) //gcs:将 (1,ct.toString.length - 1)之间的字符串都提取出来。也就是把ct字段中的{}之外的数据都取出来

              //==========================================================n2-2
              /*
              *gcs:
              *拆分ctString这个字段，
              */
              val splitComma = ctString.split(",") //{\"ald_link_key\":\"4c7030fa684dc931\"}", //gcs:将csString 使用","号切分开.将事件参数组提取出来
                                                  // 这当中的ald_link_key 和4c7030fa684dc931 分别是一个事件的事件参数。
                                                  //每一个事件，有两部分组成。{\"ald_link_key\":\"4c7030fa684dc931\"}"就是一个事件，这个事件由ald_link_key+4c7030fa684dc931 两部分组成
              //"ct":"{'distributorsID':'zilin','scene':1007}

              for (i <- 0 until splitComma.size) {

                val splitColon = splitComma(i).split(":") //{\"ald_link_key\":\"4c7030fa684dc931\"}",将这两个事件参数ald_link_key和4c7030fa684dc931事件参数分隔开。

                if (splitColon.size > 1) { //gcs:splitColon 当中存储着当前事件的所有的事件参数，如果splitColon.size>1 说明当前的这个时间是有具体的参数的。否则的话这个事件就是没有参数的
                  //取出事件的名称"ct":"{\"ald_link_key\":\"4c7030fa684dc931\"}",
                  if (splitColon(0).contains("\'")) {  //gcs:"/'"是反义字符的含义。如果 splitColon(0) 当中包含 单引号，"'"。此时就会把单引号去除掉
                    if (splitColon(0).length > 1)
                      a = splitColon(0).substring(1, splitColon(0).length - 1)
                  } else {
                    a = splitColon(0)
                  }


                  //==========================================================n2-3
                  /*
                  *gcs:
                  *事件的值是从我们的字段st当中取出的
                  */
                  //取出事件的值
                  if (splitColon(1).contains("\'")) {
                    if (splitColon(1).length > 1)
                      b = splitColon(1).substring(1, splitColon(1).length - 1)
                  } else {
                    b = splitColon(1)
                  }

                  //==========================================================n2-4
                  /*
                  *gcs:
                  *将事件的key和value进行拼接
                  */
                  //事件key 和 value 通过 : 拼接
                  str = a + ":" + b


                  //放到map中 通过 "."来拼接key
                  map.put(ak + "." + tp + "." + str, "0")
                } else { //gcs:如果 splitColon.size <= 1。说明当前 i 的这个事件参数是没有用户自定义的事件参数的。这里就会使用 0 的位置的splitComma进行赋值
                  //==========================================================n2-5
                  /*
                  *gcs:
                  *获得splitComma当中的符号
                  */
                  //去
                  if (splitComma(0).contains("\'")) { //gcs:我们提取到的每一个事件参数，有很多是前面带有 '\''的。//'ald_link_key':'4c7030fa684dc931'
                    if (splitComma(0).length > 1)
                      a = splitComma(0).substring(1, splitComma(0).length - 1)   //gcs:a = paramName：paramValue
                  } else {
                    a = splitComma(0)
                  }
                  if (splitComma(1).contains("\'")) {
                    if (splitComma(1).length > 1)
                      b = splitComma(1).substring(1, splitComma(1).length - 1)
                  } else {
                    b = splitComma(1)
                  }

                  str = a + ":" + b
                  map.put(ak + "." + tp + "." + str, "0")
                }
              }
            } else { //gcs: ct等于{}

              //gcs:将ak和tp存放到map当中
              map.put(ak + "." + tp + "." + "ct:value_null", "0")
            }

          } else { //处理不包括 "{"号的 ct 字段

            val a = "ct" + ":" + jsonLine.get("ct")
            map.put(ak + "." + tp + "." + a, "0")
          }
        }else{  //gcs:如果我们得到的数据中，ct字段是null。此时就直接把null 添加到我们的ak+tp+ct 当中去，就可以了
          val c = "ct" + ":" + jsonLine.get("ct")
          map.put(ak + "." + tp + "." + c, "0")
        }
      }


      //gcs:这个map当中存储了这个事件的所有的参数。
      //gcs:把map当中存储的所有的keys都变成（k1,k2,k3,k4....）的字符串。之后将（k1,k2,k3,k4....） 当中你的前后的小括号都去掉。变成 k1,k2,k3,k4.....
      map.keys.toString().substring(4, map.keys.toString().length - 1)
    })
      //==========================================================n2-6
      /*
      *gcs:
      *将数据按照flatMap的方法进行切分。将数据封装完成之后再去进行运算的操作
      */
      .flatMap(_.split(", ")) //统计每个map key的数量 //gcs:将 k1,k2,k3,k4,k5....按照","号切分开，形成了(k1,k2,k3,k4,k5....)的结果
      .map((_, 1)) //gcs:形成了(key1,1)、(key2,1)、(key3,1)、(key4,1)..... ；其中key1 =>(ak + "." + tp + "." + str) 这样的形式。所以(key1,1) =>((ak+"."+tp+"."+str),1) ，str=该事件(tp)下的"事件参数的key+事件参数的value"
      //gcs:即 str =事件参数name:事件参数的value.

      //==========================================================n2-7
      /*
      *gcs:
      *这里面为什么会有一个reduceByKey的操作呢？？？？？
      */
      .reduceByKey(_ + _) //gcs:之后将ak+tp+str相等的，数值1进行累加，这样就找到了每一个事件下的各个参数的值
//      .filter(!_._1.contains("ct:null"))
      .distinct()  //gcs:？？？？这里为什么要对所有的事件进行去重的操作呢？？？？ 明明算出来的就是各个事件下的参数的值啊！




    /**<br>gcs:<br>
      * 限制存储在数据库当中的b0的长度最多为255个字符
      * @param str 用于提取前255个字符的数据
      * @return 将str的前255个字符提取出来
      * */
    def  str255(str:String):String={
      var str0=str
      if(str !=null && str.length>255)
        str0=str.substring(0,254)
      //      if(str!=null && str.equals("凉感网眼V领T恤-男"))
      //        str0=null
      str0
    }



    //==========================================================n2-8
    /*
    *gcs:
    *这个jsonRDD当中存储的就是，事件下的各参数的累加的结果
    */
    //evname:evvalue ->"ald_link_key\":\"4c7030fa684dc931\"}
    val rsRDD = jSONRDD.map(x => {  //gcs:x =>(ak+"."+tp+"."+str,count【这个参数下的所有的1的累加的结果】)

      val a = x._1.split("\\.") //gcs:按照"."对ak+"."+tp+"."+str进行切割。a(0)=ak，a(1)=tp，a(2)=事件参数的值
      //gcs: ak + "." + tp + "." + evname:evvalue, evNameNum
      val b = a(2).split(":")  //gcs:b是将str按照":"分割开之后的结果。因为一个参数由两部分组成。b当中就存储了这两部分了，即 笔记n2-2 位置的"ald_link_key"，"4c7030fa684dc931"
      if (b.length >= 2) {
        val b0 = str255(b(0))  //gcs:将b(0) 对于255之外的字符全部都读取出来
        val b1 = str255(b(1))
        Row(a(0), a(1), a(0) + a(1), b0, b1, x._2) //(ak,evtype,ak+tp(evtype),evname,evvalue,evNameNum) //gcs:(ak,tp,evname+evvalue,evname,evvalue,count) //gcs:(ak,tp,ald_link_key+4c7030fa684dc931,ald_link_key,4c7030fa684dc931,count【这个参数下的所有的1的累加的结果】)
      } else {
        val b0 = str255(b(0))
        Row(a(0), a(1), a(0) + a(1), b0, "value_null", x._2)

      }
    })


    //==========================================================n2-9
    /*
    *gcs:
    *创建一个表结构。待会儿会把rsRDD映射为一个数据库的表
    */
    //创建表结构 {ak,tp,ss,ct_key,ct_value,ct_count}
    val schema = StructType(List( //gcs:创建一个数据库表
      StructField("ak", StringType, nullable = true), //gcs:常见一个数据库表中的一个字段 上面笔记n2-8的a(0)
      StructField("tp", StringType, nullable = true), //gcs:a(1)
      StructField("ss", StringType, nullable = true), //gcs:a(0) + a(1)
      StructField("ct_key", StringType, nullable = true), //gcs: b0
      StructField("ct_value", StringType, nullable = true), //gcs:b1
      StructField("ct_count", IntegerType, nullable = true)  //gcs:这个ct_count就是上边程序的 count。每一个时间参数的count的累积和【这个参数下的所有的1的累加的结果】
    ))

    //==========================================================n2-10
    //将数据和元信息匹配起来
    val oneDF = ss.createDataFrame(rsRDD, schema) //gcs:将上边的计算了一个事件的所有参数的rsRDD和一个数据库的表结构联系起来

    oneDF.createTempView("result") //gcs:将上边的rsRDD和数据库表联系起来之后的oneDF创建了一个临时的视图result


    //==========================================================n2-11
    /*
    *gcs:
    *创建自己的自定义函数
    */

    //gcs:注册一个udf。udf是什么意思啊？？？？
    //gcs:
    /*gcs:
    *sparkSession 对象用于创建一个新的对象 “collection 函数，用于用户自定义函数”
    * 创建一个用户的自定义的函数。这个创建的自定义的函数，可以作为MySql的库函数来进行使用。这个函数的名字叫做UDF
    */
    ss.udf.register("udf", (s: String) => toHex(MessageDigest.getInstance("MD5").digest(s.getBytes("UTF-8"))))


    /*
    *gcs:
    * X 表示以十六进制形式输出
    * 02 表示不足两位，前面补0输出；如果超过两位，则实际输出
    * 举例：
    * printf("%02X", 0x345);  //打印出：345
    * printf("%02X", 0x6); //打印出：06


    * %x表示按16进制输出；
    * int a = 16;
    * %02x:输出10；
    * %03x:输出：010；  表示以16进制输出，最终的结果要有3位，如果不足3为，此时要在前面补充0
    * %04x:输出：0010；
    */
    def toHex(bytes: Array[Byte]): String = bytes.map("%02x".format(_)).mkString("") //gcs:先使用 %02x 作为模板去匹配map函数当中的每一个元素。
    // 之后将map之后的结果，使用mkString将所有的字符都使用"" 分隔开



    //==========================================================n2-12
    /*
    *gcs:
    *ak:用户的app_key
    * tp:用户的事件id
    * ss:evname+evvalue 这里一定要注意啊，在进行sql的查询的时候这里使用了MD5的一个toHex的操作
    * ct_key: evname。事件的参数是由两部分组成的。evname就是第一部分。笔记n2-2 位置的"ald_link_key"，"4c7030fa684dc931" 当中的"ald_link_key"
    * ct_value：evalue。事件参数的第二部分。笔记n2-2 位置的"4c7030fa684dc931"
    * ct_count: count【这个参数下的所有的1的累加的结果】
    * now():当前的时间
    */
    val result: DataFrame = ss.sql("select ak,tp,udf(ss),ct_key,ct_value,ct_count,now() from result") //gcs:从视图result当中查数据

    //==========================================================n2-13
    /*
    *gcs:
    *将计算的结果存储到MySql当中
    */
    //gcs:将创建好的源数据存储到MySql数据库当中
    new eventParasInsert2Mysql(result, yesterday, du).insertToMysql()
  }

}

private class eventParasInsert2Mysql(result: DataFrame, yesterday: String, du: String) extends Serializable {

  def insertToMysql(): Unit = {

    result.foreachPartition((row: Iterator[Row]) => {
      val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
      var sqlText = ""

      row.foreach(r => {
        val app_key = r.get(0).toString //gcs:ak
        val day = yesterday  //gcs:注意这里的day存储的就是 运行jar包的时候，"-d xxxx" 当中的参数
        val ev_id = r.get(1).toString //gcs:tp

        val event_key = r.get(2).toString //gcs:udf(ss)

        val ev_paras_name = r.get(3).toString //ct_key

        var ev_paras_value = r.get(4).toString //gcs:ct_value
        val ef = new EmojiFilter()
        //ct_value
        val isEmoji = ef.isSpecial(ev_paras_value) //gcs:用于过滤特殊的表情和符号的
        if (isEmoji) { //gcs:判断当中是否包含特殊的表情符号
          ev_paras_value = ef.filterSpecial(ev_paras_value) //gcs:如果ef当中包含特殊符号的话，就会把所有的符合
          println(s"包含表情和特殊字符${r.toString()} 去掉表情和特殊字符=> $ev_paras_value")
        }
        val ev_paras_count = r.get(5).toString.toInt //gcs:count【这个参数下的所有的1的累加的结果】
        val update_at = r.get(6).toString    //gcs:这个update_at是MySql当中的now()函数。即当下的这条MySql执行的时候的时间，即程序执行的时候
        if (du == "") { //gcs:-du,会根据du字段来判断往哪个表当中存储数据
          sqlText =
            s"""
               |insert into aldstat_event_paras(app_key,day,ev_id,event_key,ev_paras_name,ev_paras_value,ev_paras_count,update_at)
               |values (?,?,?,?,?,?,?,?)
               |ON DUPLICATE KEY UPDATE ev_paras_count=? ,update_at= ?
                      """.stripMargin
          params.+=(Array[Any](app_key, day, ev_id, event_key, ev_paras_name, ev_paras_value, ev_paras_count, update_at, ev_paras_count, update_at))
        } else {
          sqlText = s"insert into aldstat_${du}days_event_paras (app_key,day,ev_id,event_key,ev_paras_name,ev_paras_value,ev_paras_count,update_at)values (?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE ev_paras_count=?,update_at=?"
          params.+=(Array[Any](app_key, day, ev_id, event_key, ev_paras_name, ev_paras_value, ev_paras_count, update_at, ev_paras_count, update_at))
        }

      })

      JdbcUtil.doBatch(sqlText, params) //批量入库MySql
    })
  }

}
