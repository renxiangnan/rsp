package sparkstreaming.producer

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

import kafka.serializer.Encoder
import kafka.utils.VerifiableProperties


/**
  * Created by xiangnanren on 25/05/16.
  */
class MessageEncoder[T](props: VerifiableProperties = null) extends Encoder[T] {
  override def toBytes(t: T): Array[Byte] = {
    if (t == null)
      null
    else {
      var bo: ByteArrayOutputStream = null
      var oo: ObjectOutputStream = null
      var byte: Array[Byte] = null
      try {
        bo = new ByteArrayOutputStream()
        oo = new ObjectOutputStream(bo)
        oo.writeObject(t)
        byte = bo.toByteArray
      } catch {
        case ex: Exception => return byte
      } finally {
        bo.close()
      }
      byte
    }
  }
}