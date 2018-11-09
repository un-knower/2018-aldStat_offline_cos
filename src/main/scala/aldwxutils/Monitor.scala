package aldwxutils

/**
  * Created by wangtaiyang on 2018/4/12.
  * 此类用于监控spark程序执行时间过长，停止当前spark程序
  */

class Monitor() extends Thread {
  override def run(): Unit = {
    if (true) {
      do {
        Thread.sleep(Monitor.heartbeat)
        val currentTimestamp = System.currentTimeMillis()
        val timeOfDuration = (currentTimestamp - Monitor.taskStartTime) / 1000 //60//60
        if (timeOfDuration > 20) {
          println("运行时间大于20s，异常退出")
          System.exit(1)
        }
      } while (true)
    }
  }

  def task(): Unit = {
    for (i <- 0 to 10) {
      Thread.sleep(1000)
      println(i)
    }
  }
}

object Monitor {
  //心跳时间
  private final val heartbeat = 6000
  //任务开始时间
  private final val taskStartTime = System.currentTimeMillis()

  def main(args: Array[String]): Unit = {
    val a = new Monitor()
    a.start()
    a.task()
  }
}


