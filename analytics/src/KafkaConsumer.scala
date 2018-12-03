import java.util
import java.util.UUID

import io.circe.generic.auto._
import io.circe.java8.time._
import io.circe.parser._
import models.{ActionEvent, UserId}
import org.apache.kafka.common.serialization.Deserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._

class KeyDeserializer extends Deserializer[UserId] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}
  override def deserialize(topic: String, data: Array[Byte]): UserId =
    UserId(UUID.fromString(new String(data, models.charSet)))
  override def close(): Unit = {}
}

class ValueDeserializer extends Deserializer[ActionEvent] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}
  override def deserialize(topic: String, data: Array[Byte]): ActionEvent = {
    val str          = new String(data, models.charSet)
    val errorOrEvent = parse(str + "\n").right.flatMap(_.as[ActionEvent])
    println(s"value: ${str}, ${errorOrEvent}")
    errorOrEvent.right.get
  }
  override def close(): Unit = {}
}

class KafkaConsumer(hosts: String, topic: String, streamingContext: StreamingContext) {

  private val kafkaParams = Map[String, Object](
    "bootstrap.servers"  -> hosts,
    "key.deserializer"   -> classOf[KeyDeserializer],
    "value.deserializer" -> classOf[ValueDeserializer],
    "group.id"           -> "use_a_separate_group_id_for_each_stream",
    "auto.offset.reset"  -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val stream: DStream[(UserId, ActionEvent)] = KafkaUtils
    .createDirectStream(
      streamingContext,
      PreferConsistent,
      Subscribe[UserId, ActionEvent](Array(topic), kafkaParams)
    )
    .map(record => record.key -> record.value)

}
