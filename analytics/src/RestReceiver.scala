import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import models.Event
import org.apache.log4j.Logger
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import scala.concurrent.Await
import scala.concurrent.duration._

case class RestReceiver() extends Receiver[Event](StorageLevel.MEMORY_AND_DISK_2) {

  private def log = org.apache.log4j.Logger.getLogger(this.getClass)

  override def onStart(): Unit = {
    implicit val sys: ActorSystem       = ActorSystem("RestReceiver")
    implicit val mat: ActorMaterializer = ActorMaterializer()
    import concurrent.ExecutionContext.Implicits.global
    log.debug("Receiver started...")
    val server = EventServer(event => {
      log.debug(s"Storing event: ${event}")
      store(event)
    })

    val serverBind = server.bindToPath(relativePath = "", bindToPort = 9000)

    val _ = scala.sys.addShutdownHook {
      log.debug("Unbinding...")
      val res = Await.result(serverBind.flatMap(_.unbind), 2.seconds)
      log.debug(res.toString)
    }

  }
  override def onStop(): Unit =
    log.debug("Receiver stopped...")
}
