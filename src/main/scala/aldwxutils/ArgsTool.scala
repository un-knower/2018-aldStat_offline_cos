//==========================================================f3

package aldwxutils

import org.apache.spark.sql.{DataFrame, SparkSession}

object ArgsTool {
  var day: String = new DateTimeUtil().getUdfDate(-1, "yyyy-MM-dd")  //gcs:day 的初始值是昨天的数据
  var hour = ""
  var ak = ""
  var vak = ""
  var partition = 0
  var du = ""


  //==========================================================3
  /*gcs:
  *转自,f1,3
  */
  /**
  * <b>author:</b> gcs <br>
  * <b>data:</b> 18-5-3 <br>
  * <b>description:</b><br>
    *   分析args当中的参数。将-d,-h,-ak,-vak等信息解析出来 <br>
    *     如果在args当中有"-d 2018-05-10 -du 7" 的参数。那么day就会被赋值为 2018-05-10 ，同时du 被赋值成 7
  * <b>param:</b><br>
    *   args: Array[String] 传回来的形参参数 <br>
  * <b>return:</b><br>
    *   null <br>
  */
  def analysisArgs(args: Array[String]): Unit = {
    args.foreach {
      case "-d" => day = args(args.indexOf("-d") + 1) //gcs: 对args当中的参数进行分析。“-d 2017-05-03”。代表从某一天开始对数据进行分析
      case "-h" => hour = args(args.indexOf("-h") + 1) //gcs:-h 代表，从某一个小时开始对数据开始进行分析
      case "-ak" => ak = args(args.indexOf("-ak") + 1) //gcs:-ak，代表从ak字段开始对数据进行分析
      case "-vak" => vak = args(args.indexOf("-vak") + 1) //gcs:从vak字段开始对数据进行分析
      case "-np" => partition = args(args.indexOf("-np") + 1).toInt //gcs:对某一个partition进行分析
      case "-du" => du = args(args.indexOf("-du") + 1)  //gcs:"-du 7" 指定返回数据为7天或者30天，指分析 从当前时间-7的时间的数据
      case any => println(s"接收到的参数：$any")
    }
    println(s"initialize args：day=$day,hour=$hour,partition=$partition,du=$du,ak=$ak,vak=$vak")
  }



  /**
  * <b>author:</b> gcs <br>
  * <b>data:</b> 18-5-7 <br>
  * <b>description:</b><br>
    *   从ArgsTool.day开始获得 "-du num"或者"numDays"之前的天的数据，numDays是这个函数传进来的第3个参数
    *   如果du或者numDay天的值不为0，就返回logPath路径下的numDay天的数据<br>
    *     如果du和numDay同时为0,，那么就从logPath当中读取一天的数据，即今天的数据 <br>
    *       如果du =0。但是numDays不为0，那么此时就会从logPath当中读取numDays天的数据 <br>
    *         如果 du和numDays同时不为0，那么此时就会从logPath当中读取du天的数据 <br>
  * <b>param:</b><br>
    *   args: Array[String] ;这个args中存储着天数 <br>
    *     spark: SparkSession ; sparkSession对象 <br>
    *       logPath: String ;要读取的log日志的参数 <br>
    *         numDay: String = "" ;获得天数 <br>
  * <b>return:</b><br>
    *   DataFrame ；获得读取到的目标parquet数据的集合 <br>
  */
  def getLogs(args: Array[String], spark: SparkSession, logPath: String, numDay: String = ""): DataFrame = {

    analysisArgs(args)
    if (du == "" && numDay == "") { //gcs:如果du="" 并且numDay=""，说明提交jar包的人没有提交du和numDay的天数参数。
      println(s"获取$day 数据")
      getDailyDataFrame(spark, logPath) //gcs:此时默认的就是获得daily的数据

    } else if (du == "" && numDay != "") {  //gcs:如果du=="" 但是numDay!=""
      println(s"获取昨日往前${numDay}日数据")
      getSevenOrThirtyDF(spark, logPath, numDay) //gcs:从ArgsTool.day 开始，获得ArgsTool.day 之前的numDay的数据。

    } else { //gcs如果du和numDay都不为null，此时就会获得du指定的数据
      println(s"获取${day}往前${du}天的数据")
      getSevenOrThirtyDF(spark, logPath, du)
    }
  }



  def getTencentLogs(args: Array[String], spark: SparkSession, logPath: String, numDay: String = ""): DataFrame = {
    analysisArgs(args)
    //if (du == "" && numDay == "") {
    println(s"获取$day 数据")
    //getDailyDataFrame(spark, logPath)
    getTencentDailyDataFrame(spark, logPath)

    //    } else if (du == "" && numDay != "") {
    //      println(s"获取昨日往前${numDay}日数据")
    //      getSevenOrThirtyDF(spark, logPath, numDay)
    //
    //    } else {
    //      println(s"获取${day}往前${du}天的数据")
    //      getSevenOrThirtyDF(spark, logPath, du)
    //    }
  }

  //备份 开始-----------
  /**<br>gcs:<br>
    * 从logPath目录下的-d那一天开始读取-du天的数据 <br>
    * @param args job在运行过程中的args的数据参数
    * @param spark 用于读取数据的sparkSession对象
    * @param logPath 指定从哪一个目录当中读取数据
    * @param numDay 指定从logPath目录下读取多少天的数据
    * @return 这个函数读取的数据会被封装在一个DataFrame对象当中，作为返回值返回出来。
    * */
  def getTencentLogs_bak(args: Array[String], spark: SparkSession, logPath: String, numDay: String = ""): DataFrame = {
    analysisArgs(args)
    //if (du == "" && numDay == "") {
    println(s"获取$day 数据")
    //getDailyDataFrame(spark, logPath)
    getTencentDailyDataFrame(spark, logPath) //gcs:从logPath路径下读取一天的parquet数据

    //    } else if (du == "" && numDay != "") {
    //      println(s"获取昨日往前${numDay}日数据")
    //      getSevenOrThirtyDF(spark, logPath, numDay)
    //
    //    } else {
    //      println(s"获取${day}往前${du}天的数据")
    //      getSevenOrThirtyDF(spark, logPath, du)
    //    }
  }
    //备份结束-------


  //==========================================================4
  /*gcs:
  *从path路径下的-d那一天开始读取-du天的数据
  */
  /**
    *从path路径下获得从-d 那一天开始的-du天的数据 <br>
    * @param path hdfs路径 hdfs://xxxx/ald_log_parquet
    * @return 返回每日的数据（DataFrame）
    */
  def getDailyDataFrame(spark: SparkSession, path: String): DataFrame = {

    //2017-12-20 -> 20171220
    val dayDir = day.split("-").mkString("") //gcs:把2017-12-20 -> 20171220
    println(s"$day 转换为 $dayDir")
    var logPath = ""
    logPath = s"$path/$dayDir/*$hour/*" //gcs:根据这个路径去HDFS当中读数据
    println(s"获取${logPath}的数据")
    var logs = spark.read.option("mergeSchema", "true").parquet(logPath) //gcs:通过读取parquet的格式把logPath目录中的文件读取出来

    if (ak != "") {
      val aks = ak.split(",").mkString("'", "','", "'")
      println(s"过滤出${aks}的数据")
      logs = logs.filter(s"ak in ($aks)")
      //logs.show(false)
    } else if (vak != "") {
      val vaks = vak.split(",").mkString("'", "','", "'")
      println(s"过滤掉${vaks}的数据")
      logs = logs.filter(s"ak not in ($vaks)")
      //logs.show(false)
    }
    if (partition > 0) logs.repartition(partition) else logs  //gc

  }

  /**
    * 获得path目录下的获取从-d 那一天开始的 -du参数指定的天数的parquet数据。 <br>
    * cosn://aldwxlogbackup/log_parquet/zzcg-etl-VM-0-224-ubuntu2017120313/ <br>
    * cosn://aldwxlogbackup/log_parquet/星2017120313/ <br>
    * @param spark sparkSession对象
    * @param path 要读取数据的目录
    * @return 从path目录下读取出log日志的数据被封装成为了DataFrame对象
    */
  def getTencentDailyDataFrame(spark: SparkSession, path: String): DataFrame = {
    //2017-12-20 -> 20171220
    val dayDir = day.split("-").mkString("")
    println(s"$day 转换为 $dayDir")
    val logPath = s"$path/*$dayDir$hour*/*"
    println(s"获取${logPath}的数据")
    var logs = spark.read.option("mergeSchema", "true").parquet(logPath)

    if (ak != "") {
      val aks = ak.split(",").mkString("'", "','", "'")
      println(s"过滤出${aks}的数据")
      logs = logs.filter(s"ak in ($aks)")
      //logs.show(false)
    } else if (vak != "") {
      val vaks = vak.split(",").mkString("'", "','", "'")
      println(s"过滤掉${vaks}的数据")
      logs = logs.filter(s"ak not in ($vaks)")
      //logs.show(false)
    }
    if (partition > 0) logs.repartition(partition) else logs

  }


  /**
  * <b>author:</b> gcs <br>
  * <b>data:</b> 18-5-11 <br>
  * <b>description:</b><br>
    *   读取path路径下的numDays的数据 <br>
  * <b>param:</b><br>
    *   spark: SparkSession ; <br>
    *     path: String ;要获得数据的根路径。hdfs路径 hdfs://xxxx/ald_log_parquet <br>
    *       numDays: String ;获得这个path路径下的numDays天的数据 。numDay当且仅当可以取 7或30<br>
  * <b>return:</b><br>
    *   DataFrame ；返回7日或者30日的数据 <br>
    *     将path路径下的numDays天的数据提取出来之后，之后按照ak和vaks进行筛选，将获得的parquet数据封装到这个DataFrame当中 <br>
  */
  def getSevenOrThirtyDF(spark: SparkSession, path: String, numDays: String): DataFrame = {


    val listDirs = new DateTimeUtil().getUdfDaysDirs(path, day, numDays.toInt)
    println(s"读取${numDays}天数据")
    var logs = spark.read.option("mergeSchema", "true").parquet(listDirs: _*)

    if (ak != "") { //gcs:这个ak是个啥？？？
      val aks = ak.split(",").mkString("'", "','", "'")

      println(s"过滤出${numDays}天${aks}的数据")

      logs = logs.filter(s"ak in ($aks)")
    } else if (vak != "") { //gcs:这个vak是什么意思啊？？？
      val vaks = vak.split(",").mkString("'", "','", "'")
      println(s"过滤掉${numDays}天${vaks}的数据")
      logs = logs.filter(s"ak not in ($vaks)")
    }

    //gcs:对DataFrame数据进行重新的分区
    if (partition > 0) logs.repartition(partition) else logs
  }

  /**
    *
    *
    *
    */
  /**
  * <b>author:</b> gcs <br>
  * <b>data:</b> 2017120313 <br>
  * <b>description:</b><br>
    *  弹性mr日志目录，获取7天或30天数据 <br>
    *    cosn://aldwxlogbackup/log_parquet/zzcg-etl-VM-0-224-ubuntu2017120313/ <br>
    *    cosn://aldwxlogbackup/log_parquet/星/ <br>
  * <b>param:</b><br>
    *   args: Array[String] <br>
    *     spark: SparkSession <br>
    *       numDays: String 获得numDays天的数据 <br>
  * <b>return:</b><br>
    *   DataFrame <br>
  */
  def getTencentSevenOrThirtyDF(args: Array[String], spark: SparkSession, numDays: String): DataFrame = {

    analysisArgs(args) //gcs:解析输入的参数
    val listDirs = new DateTimeUtil().getTencentDirs(day, numDays.toInt) //gcs:解析出要读取的7天数据的目录。这是一个Array数组

    println(s"读取Tencent${numDays}天数据")
    var logs = spark.read.option("mergeSchema", "true").parquet(listDirs: _*) //gcs:从logs这些目录文件夹中将json格式的文件都读取出来

    if (ak != "") {
      val aks = ak.split(",").mkString("'", "','", "'")
      println(s"过滤出${numDays}天${aks}的数据")
      logs = logs.filter(s"ak in ($aks)")
    } else if (vak != "") {
      val vaks = vak.split(",").mkString("'", "','", "'")
      println(s"过滤掉${numDays}天${vaks}的数据")
      logs = logs.filter(s"ak not in ($vaks)")
    }
    if (partition > 0) logs.repartition(partition) else logs
  }

  /**
  * <b>author:</b> gcs <br>
  * <b>data:</b> 18-5-4 <br>
  * <b>description:</b><br>
    *   从day开始读取numDays天的数据。day如果没有指定时间的话，day默认是昨天的时间 <br>
    *     并且同时把读取出来的数据使用ak和vak字段进行过滤 <br>
  * <b>param:</b><br>
    *   spark: SparkSession ；用于读取数据 <br>
    *     numDays: String ;需要读取numDays的数据。现在的numdays只取7或者30天 <br>
  * <b>return:</b><br>
    *    DataFrame ; 将numDays路径中的数据读取出来，之后放在DataFrame当中 <br>
  */
  def getTencentSevenOrThirty( spark: SparkSession, numDays: String): DataFrame = {


    /*gcs:
    *如果你的main函数中的args当中有参数的话，"-d 2017-05-10" 因为前面的程序已经将为day赋值了。所以day的值会是 20170510
    * 如果不指定"-d xxxx"的参数，day的默认值是 昨天的数据
    */
    val listDirs = new DateTimeUtil().getTencentDirs(day, numDays.toInt) //gcs:获得等待处理数据的工作路径

    println(s"读取Tencent${numDays}天数据")

    //gcs:从所有的天数的路径中把数据读取出来
    var logs = spark.read.option("mergeSchema", "true").parquet(listDirs: _*)


    if (ak != "") { //gcs:如果ak不等于null，就代表只需要处理这个ak的数据就可以了，其他不是这个ak的数据就可以删除掉了。
      val aks = ak.split(",").mkString("'", "','", "'")
      println(s"过滤出${numDays}天${aks}的数据")
      logs = logs.filter(s"ak in ($aks)")
    } else if (vak != "") { //gcs:如果vak不为null，此时就根据vaks将数据过滤出来
      val vaks = vak.split(",").mkString("'", "','", "'")
      println(s"过滤掉${numDays}天${vaks}的数据")
      logs = logs.filter(s"ak not in ($vaks)")
    }
    if (partition > 0) logs.repartition(partition) else logs
  }



  /**
  * <b>author:</b> gcs <br>
  * <b>data:</b> 18-5-7 <br>
  * <b>description:</b><br>
    *   ArgsTool是一个参数类。<br>
    *  从ArgsTool.day那天开始，往前读取daysArr数组中的第ArgsTool.day-dayArr(i)天的数据。<br>
    *    之后将这些所有的天数据再拼凑成集合，存储在DataFrame当中，在返回去 <br>
  * <b>param:</b><br>
    *   args: Array[String] ; -d 2017-05-03 。args这个参数用来提取day 的值，指定从哪一天开始读取数据<br>
    *     spark: SparkSession ;用来读取数据的sparkSession 对象 <br>
    *       daysArr: Array[Int] ；将要去读取哪些天数的数组 <br>
  * <b>return:</b><br>
    *   DataFrame ;将 daysArr数组中的天数的数据提取出来之后，统一地封装进这个DataFrame对象当中，再返回出去 <br>
  */
  def getSpecifyTencentDateDF(args: Array[String], spark: SparkSession, daysArr: Array[Int]): DataFrame = {

    //gcs:首先在程序的这一行，day就会从"-d 2018-05-10"，du的值就会从"-du 7" ak 就会从 "-ak xxxxxxx" 中被附上值
    //gcs:如果没有指定"-d 2018-05-10"，那么day的值就是昨天的日期
    analysisArgs(args) //gcs:对args传入的数据进行分析

    val listDirs = new DateTimeUtil().getSpecifyTencentDirs(day,daysArr) //gcs:listDirs 代表要读取的n天数据所存储的路径的数组

    println(s"读取Tencent${daysArr}天数据")

    //gcs:把要读取的天数读取到logs当中
    var logs = spark.read.option("mergeSchema", "true").parquet(listDirs: _*) //gcs:listDirs: _* 是指将listDirs当做一个数组来对待

    if (ak != "") { //gcs:ak,微信小程序的唯一标识。将指定ak下的数据都提取出来
      val aks = ak.split(",").mkString("'", "','", "'")

      println(s"过滤出${daysArr}天${aks}的数据")

      logs = logs.filter(s"ak in ($aks)") //gcs:将指定ak的数据都提取出来
    } else if (vak != "") { //gcs:vak 是什么参数呢？？？？

      val vaks = vak.split(",").mkString("'", "','", "'")
      println(s"过滤掉${daysArr}天${vaks}的数据")
      logs = logs.filter(s"ak not in ($vaks)")
    }
    if (partition > 0) logs.repartition(partition) else logs
  }
}