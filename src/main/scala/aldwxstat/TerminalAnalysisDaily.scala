//==========================================================
/*gcs:
*终端分析，计算当天的数据，每天晚上AM 2:25跑一次
*/

package aldwxstat

import java.sql.Timestamp

import aldwxconfig.ConfigurationUtil
import aldwxutils.{ArgsTool, JdbcUtil, TimeUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by 18510 on 2017/8/22. 终端分析
  */
object TerminalAnalysisDaily {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val sparkseesion = SparkSession.builder()
      .appName(this.getClass.getName)
      .getOrCreate()

    //==========================================================1
    //获取昨天时间yyyyMMdd
    val UpDataTime = new Timestamp(System.currentTimeMillis()) //gcs:获得当前的时间

    val yesterday = TimeUtil.processArgs(args) //gcs:对传入的参数进行处理 yesterday 就是"-d 2017-05-07" 中的20170507


    val dateTime = TimeUtil.getTimestamp(yesterday) //gcs:String类型的时间数据转换成Timestamp类型的



    //==========================================================1.1
    /*
    *gcs:
    *计算各个维度下的指标
    */
    /*gcs:
    *“wvv" 客户端平台，例如：devtools(它是一个安卓平台下的系统)。
    *nt 网络类型 用户的手机当前连的是什么网络。这个网络可以是 数据流量，wifi
    *lang 用户在使用那种微信语言。zh_CN代表”中文“。你比如说我的微信的功能模块显示的都是英文，这就说明我选择的微信语言是”英文“。微信语言，例：zh_CN
    *wv 微信版本号 例6.5.6
    *sv 操作系统版本号 例iOS 10.0.1
    *wsdk 是客户端基础版本库。这个字段的含义是WebApp开发者使用了微信的哪个版本的SDK开发这个微信小程序。例：1.4.0
    *ww_wh 是？？？
    */
    //gcs:这个terminal_sum 数组的作用是什么呢？？？？？？ 我只知道，wvv,nt,lang,wv,sv,wsdk,ww_wh是我们公司的SDK部门采集到的几个字段
    val terminal_sum = Array("wvv", "nt", "lang", "wv", "sv", "wsdk", "ww_wh")



    //    val paths = aldwxutils.DBConf.hdfsUrl + "/"
    val paths = ConfigurationUtil.getProperty("hdfsUrl") + "/"  //gcs: 组合完路径之后，是这个样子的：hdfs://10.0.0.212:9000/ald_log_parquet/


    //    val df = Aldstat_args_tool.analyze_args(args,sparkseesion,DBConf.hdfsUrl)
    //gcs:df是根据-du numdays 指定的天数，从 hdfs://10.0.0.212:9000/ald_log_parquet 当中读取从ArgsTool.day天开始的之前的指定的天数的数据
    val df = ArgsTool.getLogs(args, sparkseesion, ConfigurationUtil.getProperty("tongji.parquet"))  //gcs:根据hdfs://10.0.0.212:9000/ald_log_parquet路径，从里面读取"-du,天数的数据"，因为这个函数中没有指定numDays的天数


    //==========================================================2
    for (terminal <- terminal_sum) {

      //==========================================================2.1
      /*
      *gcs:
      *使用同一组数据，开分别计算wvv,nt,lang,wv,sv,wsdk,ww_wh等指标
      */
      df.createOrReplaceTempView(s"phone_brand_$terminal") //gcs:把提取出来的数据，创建7个视图 phone_brand_wvv , phone_brand_nt
      var tmp_args = ""

      if (terminal == "ww_wh") { //gcs:如果terminal为ww_wh 为这个字段。ww_wh到底是什么意思啊？？

        //==========================================================2.2
        /*
        *gcs:
        *这是计算ww_wh这个维度下的所有的指标
        * 新用户数是ifo =true的uu的个数
        */
        //gcs:MySql的concat方法，将字符串进行聚合在一起.concat(ww,'*',wh) 聚合成 ww*wh。这是因为一定把ww_wh当中的_换成了*号，这个符号了。
        //新访客(new_comer_count)
        //gcs:计算新访客，因为当中的。先根据 ak进行分组，再根据 ww*wh 进行分组。根据uu,ifo计算新用户数
        val newnum = sparkseesion.sql(s"SELECT ak app_key,concat(ww,'*',wh) tmp_sum,COUNT(DISTINCT uu) new_comer_count FROM phone_brand_${terminal} WHERE ev = 'app' AND ifo='true' GROUP BY ak,concat(ww,'*',wh)")


        //==========================================================2.3
        /*
        *gcs:
        *计算访问人数。因为计算的数据和ww_wh是相关的，所以要按照ww_wh来进行分组
        */
        println("----------------------开始-----------------------")
        //        newnum.show()
        //访问人数(visitor_count)。根据uu计算访问人数
        val visnum = sparkseesion.sql(s"SELECT ak app_key,concat(ww,'*',wh) tmp_sum,COUNT(DISTINCT uu) visitor_count FROM phone_brand_$terminal WHERE ev='app' GROUP BY ak,concat(ww,'*',wh)")


        //==========================================================2.4
        /*
        *gcs:
        *计算打开次数。按照ww_wh进行分组。计算at的数目
        */
        //打开次数(open_count)，根据at计算打开次数
        /*gcs:
        *这里有一个问题。有时候会出现访问人数为0，但是访问有值的情况。这是因为访问人数是使用 uu来记性计算的，但是访问次数是使用 at来进行计算的
        */
        val opennum = sparkseesion.sql(s"SELECT ak app_key,concat(ww,'*',wh) tmp_sum,COUNT(DISTINCT at) open_count FROM phone_brand_$terminal WHERE ev='app' GROUP BY ak,concat(ww,'*',wh)")
        //        opennum.show()


        //==========================================================2.5
        /*
        *gcs:
        *按照ww_wh进行分组，计算pp的数目就是页面总和
        */
        //页面总访问量，根据 pp 计算页面总访问量
        val totalpage = sparkseesion.sql(s"SELECT ak app_key,concat(ww,'*',wh) tmp_sum,COUNT(pp) total_page_count FROM phone_brand_$terminal WHERE ev='page' GROUP BY ak,concat(ww,'*',wh)")
        //        totalpage.show()


        //总停留时长
        //concat(ww,'*',wh)。根据x 来计算总停留时长。x是什么啊？？？？
        val totaltimes = sparkseesion.sql(s"SELECT ak app_key,eeee.aaa tmp_sum,sum(x) total_stay_time FROM (SELECT ak,concat(ww,'*',wh) aaa,cast(max(st)/1000 as int)-cast(min(st)/1000 as int) x FROM phone_brand_$terminal WHERE ev='page' GROUP BY ak,at,concat(ww,'*',wh)) eeee group by ak,eeee.aaa")
        //        totaltimes.show()
        //      sparkseesion.sql(s"SELECT ak,at,${tmp_args},cast(max(st)/1000 as int)-cast(min(st)/1000 as int) x FROM phone_brand_$terminal WHERE ev='page' GROUP BY ak,at,${tmp_args}").show()



        //==========================================================3
        /*gcs:
        *创建临时表
        */
        //创建临时表
        newnum.createTempView(s"newnum1_$terminal") //gcs:新用户创建临时表
        visnum.createTempView(s"visnum1_$terminal") //gcs:访问人数的临时表
        opennum.createTempView(s"opennum1_$terminal") //gcs:打开次数的临时表
        totalpage.createTempView(s"totalpage1_$terminal")  //gcs:页面总访问量的临时表
        totaltimes.createTempView(s"totaltimes1_$terminal") //gcs:总停留时长的临时表

        //==========================================================2.5
        /*
        *gcs:
        *计算次均停留时间
        */
        //次均 //gcs:使用 visnum1_$terminal 和 totalpage1_$terminal 来计算次均时长
        val pl = sparkseesion.sql(s"SELECT a.app_key app_key,a.tmp_sum tmp_sum,cast(b.total_stay_time/a.visitor_count as int) avg_stay_time FROM visnum1_$terminal a left join totaltimes1_$terminal b on a.app_key=b.app_key and a.tmp_sum = b.tmp_sum")
        //        pl.show()
        pl.createTempView(s"pl1_$terminal")


        //==========================================================2.7
        /*
        *gcs:
        *
        */
        //访问一次页面的次数。
        val times_one2 = sparkseesion.sql(s"select tmp.ak app_key,tmp.tmp_sum_one tmp_sum,sum(tmp.cp) one_cp from (SELECT ak,at ,concat(ww,'*',wh) tmp_sum_one,COUNT(pp) cp FROM phone_brand_$terminal where ev = 'page' GROUP BY ak,at,pp,concat(ww,'*',wh)) tmp group by tmp.ak,tmp.at,tmp.tmp_sum_one")
        //        times_one2.show()
        times_one2.createTempView(s"times_one2_$terminal")


        //gcs:从访问一次页面的次数的times_one2中计算 访问了多次次数的数据
        val times_one3 = sparkseesion.sql(s"select app_key,tmp_sum,sum(one_cp) one_page_count from times_one2_$terminal where one_cp = 1 group by app_key,tmp_sum")
        times_one3.createTempView(s"times_one_$terminal")


        //页面跳出率 //gcs:从页面总访问量的表中计算页面跳出率。在进行left join之前为新产生的select语句的结果定义为了a,b,c
        val bounce = sparkseesion.sql(s"select a.app_key app_key,a.tmp_sum tmp_sum,cast(b.one_page_count/a.total_page_count as float) bounce_rate from totalpage1_$terminal a left join times_one_$terminal b on a.app_key = b.app_key and a.tmp_sum = b.tmp_sum")
        bounce.createTempView(s"bounce1_$terminal")


        //==========================================================2.8
        /*
        *gcs:
        *将每一个指标的计算结果join在一块儿。这样就可以将各个指标聚合在在一起
        */
        //gcs:
        val result = sparkseesion.sql(s"SELECT a.app_key,a.tmp_sum type_value,a.total_page_count,b.visitor_count,c.open_count,d.total_stay_time,e.avg_stay_time,f.new_comer_count,g.one_page_count,h.bounce_rate from totalpage1_$terminal " +
          s"a left join visnum1_$terminal b on a.app_key = b.app_key and a.tmp_sum = b.tmp_sum left join opennum1_$terminal c on b.app_key = c.app_key and b.tmp_sum = c.tmp_sum " +
          s"left join totaltimes1_$terminal d on c.app_key = d.app_key and c.tmp_sum = d.tmp_sum left join pl1_$terminal e on d.app_key = e.app_key and d.tmp_sum = e.tmp_sum left join newnum1_$terminal f on e.app_key = f.app_key and e.tmp_sum = f.tmp_sum left join times_one_$terminal g on g.app_key = f.app_key and g.tmp_sum = f.tmp_sum left join bounce1_$terminal h on g.app_key = h.app_key and g.tmp_sum = h.tmp_sum").distinct().na.fill(0).withColumn("type", lit(terminal))

       //==========================================================4
        /*gcs:
        *将数据插入到数据库当中
        */
        // result.show()
        result.foreachPartition((rows: Iterator[Row]) => {

          val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据

          //gcs:定义sql语句，创建表并且往里面插入数据
          val sqlText = s"insert into aldstat_terminal_analysis (app_key,day,new_comer_count, visitor_count,open_count,total_page_count,avg_stay_time,total_stay_time,bounce_rate,one_page_count,update_at,type,type_value)" +
            s"values (?,?,?,?,?,?,?,?,?,?,?,?,?) ON " +
            s"DUPLICATE KEY UPDATE new_comer_count=?, visitor_count=?, open_count=?,total_page_count=?, avg_stay_time=?,total_stay_time=?, bounce_rate=?,one_page_count=?,update_at=?,type_value=?"

          //gcs:先收集数据之后进行批量的操作
          rows.foreach(r => {
            val day = dateTime
            val update_at = UpDataTime
            val app_key = r.get(0)
            val type_values = r.get(1)
            val total_page_count = r.get(2)
            val visitor_count = r.get(3)
            val open_count = r.get(4)
            val total_stay_time = r.get(5)
            val avg_stay_time = r.get(6)
            val new_comer_count = r.get(7)
            val one_page_count = r.get(8)
            val bounce_rate = r.get(9)
            var ty = ""
            if (r.get(10).toString == "" || r.get(10).toString == "null" || r.get(10) == null || r.get(10).toString == "undefined") {
              ty = "未知"
            } else {
              ty = r.get(10).toString()
            }

            params.+=(Array[Any](app_key, day, new_comer_count, visitor_count, open_count, total_page_count, avg_stay_time, total_stay_time, bounce_rate, one_page_count, update_at, ty, type_values))
          })
          JdbcUtil.doBatch(sqlText,params)
        })
      } else {
        //==========================================================5
        tmp_args = terminal
        //新访客(new_comer_count)
        val newnum = sparkseesion.sql(s"SELECT ak app_key,${tmp_args} tmp_sum,COUNT(DISTINCT uu) new_comer_count FROM phone_brand_${terminal} WHERE ev = 'app' AND ifo='true' GROUP BY ak,${tmp_args} ")
        //访问人数(visitor_count)
        val visnum = sparkseesion.sql(s"SELECT ak app_key,${tmp_args} tmp_sum,COUNT(DISTINCT uu) visitor_count FROM phone_brand_$terminal WHERE ev='app' GROUP BY ak,${tmp_args} ")
        //打开次数(open_count)
        val opennum = sparkseesion.sql(s"SELECT ak app_key,${tmp_args} tmp_sum,COUNT(DISTINCT at) open_count FROM phone_brand_$terminal WHERE ev='app' GROUP BY ak,${tmp_args}")
        //页面总访问量
        val totalpage = sparkseesion.sql(s"SELECT ak app_key,${tmp_args} tmp_sum,COUNT(pp) total_page_count FROM phone_brand_$terminal WHERE ev='page' GROUP BY ak,${tmp_args}")
        //    totalpage.show()
        //总停留时长
        val totaltimes = sparkseesion.sql(s"SELECT ak app_key,${tmp_args} tmp_sum,sum(x) total_stay_time FROM (SELECT ak,${tmp_args},cast(max(st)/1000 as int)-cast(min(st)/1000 as int) x FROM phone_brand_$terminal WHERE ev='page' GROUP BY ak,at,${tmp_args}) group by ak,${tmp_args}")
        //      sparkseesion.sql(s"SELECT ak,at,${tmp_args},cast(max(st)/1000 as int)-cast(min(st)/1000 as int) x FROM phone_brand_$terminal WHERE ev='page' GROUP BY ak,at,${tmp_args}").show()


        //==========================================================2.2
        /*
        *gcs:
        *创建临时的视图。
        * 这些视图的开始是以 newnum1_开头的，后面添加${tmp_args}，随着处理的模块的不同，视图的名字页不相同
        * 比方说，现在是在处理wvv (系统) 这个维度，那么${terminal} =wvv ，此时创建的newnum的视图的名字就会为 newnum1_wvv
        */
        //创建临时表
        newnum.createTempView(s"newnum1_$terminal")
        visnum.createTempView(s"visnum1_$terminal")
        opennum.createTempView(s"opennum1_$terminal")
        totalpage.createTempView(s"totalpage1_$terminal")
        totaltimes.createTempView(s"totaltimes1_$terminal")
        //        newnum.show()
        //        visnum.show()
        //        opennum.show()
        //        totalpage.show()
        //        totaltimes.show()


        //==========================================================2.3
        /*
        *gcs:
        *计算次均停留时间
        */
        //次均
        val pl = sparkseesion.sql(s"SELECT a.app_key app_key,a.tmp_sum tmp_sum,cast(b.total_stay_time/a.visitor_count as int) avg_stay_time FROM visnum1_$terminal a left join totaltimes1_$terminal b on a.app_key=b.app_key and a.tmp_sum = b.tmp_sum")
        pl.createTempView(s"pl1_$terminal")

        //访问一次页面的次数
        val times_one2 = sparkseesion.sql(s"select tmp.ak app_key,tmp.tmp_sum_one tmp_sum,sum(tmp.cp) one_cp from (SELECT ak,at ,${tmp_args} tmp_sum_one,COUNT(pp) cp FROM phone_brand_$terminal where ev = 'page' GROUP BY ak,at,pp,${tmp_args}) tmp group by tmp.ak,tmp.at,tmp.tmp_sum_one")
        times_one2.createTempView(s"times_one2_$terminal")


        val times_one3 = sparkseesion.sql(s"select app_key,tmp_sum,sum(one_cp) one_page_count from times_one2_$terminal where one_cp = 1 group by app_key,tmp_sum")
        times_one3.createTempView(s"times_one_$terminal")

        //页面跳出率
        val bounce = sparkseesion.sql(s"select a.app_key app_key,a.tmp_sum tmp_sum,cast(b.one_page_count/a.total_page_count as float) bounce_rate from totalpage1_$terminal a left join times_one_$terminal b on a.app_key = b.app_key and a.tmp_sum = b.tmp_sum")
        //    pl.show()
        //    times_one3.show()
        //    bounce.show()
        bounce.createTempView(s"bounce1_$terminal")
        //    bounce.show()
        //    val result = sparkseesion.sql("SELECT " + yesterday + s" day,${UpDataTime} update_at,a.app_key,a.new_comer_count,b.visitor_count,c.open_count,d.total_page_count,e.total_stay_time,f.avg_stay_time,k.bounce_rate,a.tmp_sum type_value FROM newnum1_$tmp_args a left join visnum1_$tmp_args b on a.app_key = b.app_key and a.tmp_sum =b.tmp_sum left join opennum1_$tmp_args c on b.app_key = c.app_key and b.tmp_sum =c.tmp_sum left join totalpage1_$tmp_args d on c.app_key = d.app_key and c.tmp_sum = d.tmp_sum left join totaltimes1_$tmp_args e on d.app_key = e.app_key and d.tmp_sum = e.tmp_sum left join pl1_$tmp_args f on e.app_key = f.app_key and e.tmp_sum = f.tmp_sum left join bounce1_$tmp_args k on f.app_key=k.ak and f.tmp_sum = k.tmp_sum").withColumn("type",lit(tmp_args)).distinct().na.fill(0)


        //==========================================================2.4
        /*
        *gcs:
        *将所有的所有的指标的聚合在一起
        */
        val result = sparkseesion.sql(s"SELECT a.app_key,a.tmp_sum type_value,a.total_page_count,b.visitor_count,c.open_count,d.total_stay_time,e.avg_stay_time,f.new_comer_count,g.one_page_count,h.bounce_rate from " +
          s"totalpage1_$terminal a left join visnum1_$terminal b on a.app_key = b.app_key and a.tmp_sum = b.tmp_sum left join opennum1_$terminal c on b.app_key = c.app_key and b.tmp_sum = c.tmp_sum left join totaltimes1_$terminal d on c.app_key = d.app_key and c.tmp_sum = d.tmp_sum left join pl1_$terminal e on d.app_key = e.app_key and d.tmp_sum = e.tmp_sum left join newnum1_$terminal f on e.app_key = f.app_key and e.tmp_sum = f.tmp_sum left join times_one_$terminal g on g.app_key = f.app_key and g.tmp_sum = f.tmp_sum left join bounce1_$terminal h on g.app_key = h.app_key and g.tmp_sum = h.tmp_sum").distinct().na.fill(0).withColumn("type", lit(terminal))

        //==========================================================6
        /*gcs:
        *将数据插入进数据库
        * result和mysql库中的数据的对应关系
        * a.app_key 对应app_key，a视图对应totalpage1_$terminal
        * f.new_comer_count 对应 new_comer_count  。f对应 newnum1_$terminal 。新用户数
        * b.visitor_count 对应 visitor_count  b视图对应visnum1_$terminal 。访问人数
        * c.open_count 对应 open_count   ，c视图对应 opennum1_$terminal 打开次数
        * a.tmp_sum type_value 对应 type_value 。这个type_value的值可以取,wvv,nt,long 等维度信息。a视图对应totalpage1_$terminal
        * a.total_page_count 对应 total_page_count。a视图对应totalpage1_$terminal
        * e.avg_stay_time 对应 avg_stay_time 。e视图对应 pl1_$terminal
        *
        * d.total_stay_time 对应 total_stay_time 。d视图对应 totaltimes1_$terminal
        * h.bounce_rate 对应 bounce_rate 。h视图对应 bounce1_$terminal
        *
        * g.one_page_count 对应 one_page_count 。g视图对应 times_one_$terminal
        * a.tmp_sum type_value，a视图对应totalpage1_$terminal
        */
        //result.show()
        result.foreachPartition((rows: Iterator[Row]) => {

          val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
          val sqlText = s"insert into aldstat_terminal_analysis (app_key,day,new_comer_count, visitor_count,open_count,total_page_count,avg_stay_time,total_stay_time,bounce_rate,one_page_count,update_at,type,type_value)" +
            s"values (?,?,?,?,?,?,?,?,?,?,?,?,?) ON " +
            s"DUPLICATE KEY UPDATE new_comer_count=?, visitor_count=?, open_count=?,total_page_count=?, avg_stay_time=?,total_stay_time=?, bounce_rate=?,one_page_count=?,update_at=?,type_value=?"

          rows.foreach(r => {
            val day = dateTime
            val update_at = UpDataTime
            val app_key = r.get(0)
            val type_values = r.get(1)
            val total_page_count = r.get(2)
            val visitor_count = r.get(3)
            val open_count = r.get(4)
            val total_stay_time = r.get(5)
            val avg_stay_time = r.get(6)
            val new_comer_count = r.get(7)
            val one_page_count = r.get(8)
            val bounce_rate = r.get(9)
            val ty = r.get(10)

            params.+=(Array[Any](app_key, day, new_comer_count, visitor_count, open_count, total_page_count, avg_stay_time, total_stay_time, bounce_rate, one_page_count, update_at, ty, type_values, new_comer_count, visitor_count, open_count, total_page_count, avg_stay_time, total_stay_time, bounce_rate, one_page_count, update_at, type_values))
          })
          JdbcUtil.doBatch(sqlText,params)
        })

      }
    }
    sparkseesion.close()
  }
}
