package aldwxutils

import java.io.IOException

import org.apache.hadoop.fs.{FileSystem, Path}


/**
  * Created by wangtaiyang on 2017/12/18.
  */
class HDFS {
  //private val fs = FileSystem.get(new URI("hdfs://localhost:9000"),new Configuration())
 // private val fs = FileSystem.get(new URI(ConfigurationUtil.getProperty("tongji.parquet")), new Configuration())

  /**
    * 判断文件是否存在
    */
  def exists(fs:FileSystem,str: String): Boolean = {
    try
      fs.exists(new Path(str))
    catch {
      case e: IOException =>
        e.printStackTrace()
    }
    false
  }

  /**
    * 判断目录是否存在
    */
  def isDirectory(fs:FileSystem,path: String*): Boolean = {
    var isDirectory = false
    try {
      for (s <- path) {
        isDirectory = fs.isDirectory(new Path(s))
        if (!isDirectory) return false
      }
    }
    catch {
      case e: IOException => e.printStackTrace()
    }
    isDirectory
  }



}




