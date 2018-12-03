import java.util
import java.util.UUID

import io.circe.generic.auto._
import io.circe.syntax._
import io.circe.{Decoder, Encoder, HCursor}
import org.apache.kafka.common.serialization.Serializer
import io.circe.java8.time._

package object models {
  import io.estatico.newtype.macros.newtype

  val charSet = java.nio.charset.StandardCharsets.UTF_8

  val keySerializer: Serializer[ActionEvent] = new Serializer[ActionEvent] {
    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}
    override def serialize(topic: String, data: ActionEvent): Array[Byte] =
      data.userId.id.toString.getBytes(charSet)
    override def close(): Unit = {}
  }
  val valueSerializer: Serializer[ActionEvent] = new Serializer[ActionEvent] {
    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}
    override def serialize(topic: String, data: ActionEvent): Array[Byte]      = data.asJson.noSpaces.getBytes(charSet)
    override def close(): Unit                                                 = {}
  }

  object UserId {
    implicit val dec: Decoder[UserId] = new Decoder[UserId] {
      override def apply(c: HCursor) = c.as[UUID].right.map(id => UserId(id))
    }
    implicit val enc: Encoder[UserId] = new Encoder[UserId] {
      override def apply(a: models.UserId) = a.id.asJson
    }
  }
  @newtype case class UserId(id: UUID)

}
