import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import models.Event
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

object Main extends App {

  private val log = org.apache.log4j.Logger.getLogger(this.getClass)

  private implicit val sys = ActorSystem("analytics")
  private implicit val mat = ActorMaterializer()
  private implicit val ec  = sys.dispatcher

  private val restReceiver = RestReceiver()

  val applicationName: String       = "UserFlowAnalytics"
  val masterConfiguration: String   = "local[2]"
  val windowSizeInSeconds: Duration = Seconds(60 * 60)
  val intervalInSeconds: Duration   = Seconds(6)
  val batchInterval: Duration       = Seconds(6)

  val conf: SparkConf       = new SparkConf().setAppName(applicationName).setMaster(masterConfiguration)
  val ssc: StreamingContext = new StreamingContext(conf, batchInterval)
  val events: DStream[Event] =
    ssc.receiverStream(restReceiver).window(windowSizeInSeconds, intervalInSeconds)

  val analytics = UserFlowAnalytics()

  analytics.setup(events)

  ssc.start()
  ssc.awaitTermination()

}
