import models.{ActionEvent, Event}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

object Main extends App {

  val applicationName: String       = "UserFlowAnalytics"
  val masterConfiguration: String   = "local[2]"
  val windowSizeInSeconds: Duration = Seconds(60 * 60)
  val intervalInSeconds: Duration   = Seconds(6)
  val batchInterval: Duration       = Seconds(6)

  val conf: SparkConf       = new SparkConf().setAppName(applicationName).setMaster(masterConfiguration)
  val ssc: StreamingContext = new StreamingContext(conf, batchInterval)
  val kafkaConsumer         = new KafkaConsumer("0.0.0.0:9092", "test", ssc)
  val events: DStream[(models.UserId, ActionEvent)] =
    kafkaConsumer.stream.window(windowSizeInSeconds, intervalInSeconds)

  //    ssc.receiverStream(restReceiver).window(windowSizeInSeconds, intervalInSeconds)

  val analytics = UserFlowAnalytics()

  analytics.setup(events)

  ssc.start()
  ssc.awaitTermination()

}
