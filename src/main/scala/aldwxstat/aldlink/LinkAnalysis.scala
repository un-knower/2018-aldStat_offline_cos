package aldwxstat.aldlink

/**
  * Created by wangtaiyang on 2017/12/21. 
  */
trait LinkAnalysis {
  def newUser()
  def visitorCount()
  def openCount()
  def totalPageCount()
  def totalStayTime()
  def secondaryAvgStayTime()
  def visitPageOnce()
  def bounceRate()
  def insert2db()

}
