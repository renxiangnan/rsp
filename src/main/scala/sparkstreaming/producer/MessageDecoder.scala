package sparkstreaming.producer

/**
  * Created by xiangnanren on 25/05/16.
  */

import java.io.{ByteArrayInputStream, ObjectInputStream}

import kafka.serializer.Decoder
import kafka.utils.VerifiableProperties

class MessageDecoder[T](props: VerifiableProperties = null) extends Decoder[T] {
  def fromBytes(bytes: Array[Byte]): T = {
    var t: T = null.asInstanceOf[T]
    var bi: ByteArrayInputStream = null
    var oi: ObjectInputStream = null
    try {
      bi = new ByteArrayInputStream(bytes)
      oi = new ObjectInputStream(bi)
      t = oi.readObject().asInstanceOf[T]
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      bi.close()
      oi.close()
    }
    t
  }
}

