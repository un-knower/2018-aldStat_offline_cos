package aldwxutils

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

/**
  * Created by wangtaiyang on 2018/4/8. 
  */
class Registers extends KryoRegistrator{
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[EmojiFilter])
  }
}
