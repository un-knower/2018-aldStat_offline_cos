package aldwxutils

/**
  * Created by wangtaiyang on 2018/4/8.
  * 过滤和判断表情字符
  */
class EmojiFilter {

  /**<br>gcs:这个regex就是表情符号判断的正则表达式*/
  private final val regex="(?i)[^\\sa-zA-Z0-9\u4E00-\u9FA5)(`~～!--·@#$%^&*()+=|丿\\\\　{}':;',//[//]\\[\\].<>/?~！@#￥%……&*（）——+|{}【】《》_‘；：”“’。，、？]"

  //
  /**
  * <b>author:</b> gcs <br>
  * <b>data:</b> 6/5/18 <br>
  * <b>description:</b><br>
    *   判断一个str：String当中是否包含表情和特殊字符(不算标点符号和空格)
  * <b>param:</b><br>
  * <b>return:</b><br>
  */
  def isSpecial(str: String): Boolean = {
    var isSpecialChar = false
    for (s <- str) {
      val isMatcher = s.toString.matches(regex)  //gcs:如果这个str的字符串当中的任意的一个字符符合这个正则表达式regex，就认为str当中是包含字符串的
      if (isMatcher) isSpecialChar = true
    }
    isSpecialChar
  }


  //
  /**
  * <b>author:</b> wty <br>
  * <b>data:</b> 4/18/18 <br>
  * <b>description:</b><br>
    *   过滤表情和特殊字符(不算标点符号和空格)
  * <b>param:</b><br>
  * <b>return:</b><br>
  */
  def filterSpecial(str: String): String = {
    str.replaceAll(regex, " ") //gcs:将 str字符串中的所有符合regex正则表达式的都替换成" "
  }


}

object EmojiFilter {
  def main(args: Array[String]): Unit = {
    val ef=new EmojiFilter()
    val b="【合肥~～站】　　cium孟\\京·[辉-经典戏剧作]品《恋爱的犀牛》"
    val bool = ef.filterSpecial(b)
    println(bool)
    println(ef.isSpecial(b))
  }
}

