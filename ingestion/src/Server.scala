import akka.actor.ActorSystem
import akka.http.javadsl.common.{EntityStreamingSupport, JsonEntityStreamingSupport}
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.server.Route
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.{Done, NotUsed}
import io.circe.generic.auto._
import io.circe.java8.time._
import models.{ActionEvent, _}
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
object Server extends App {

  private implicit val system: ActorSystem          = ActorSystem("ingestion")
  private implicit val mat: ActorMaterializer       = ActorMaterializer()
  private implicit val ec: ExecutionContextExecutor = system.dispatcher
  private val bootstrapServers                      = "0.0.0.0:9092"
  private val topic                                 = "test"

  private val producerSettings: ProducerSettings[ActionEvent, ActionEvent] =
    ProducerSettings(system, keySerializer, valueSerializer)
      .withBootstrapServers(bootstrapServers)
      .withCloseTimeout(60.seconds)
      .withParallelism(16)
      .withDispatcher("akka.kafka.default-dispatcher")
      .withEosCommitInterval(100.millis)

  private val kafkaSink: Sink[ProducerRecord[ActionEvent, ActionEvent], Future[Done]] =
    Producer.plainSink(producerSettings)

  private def publishMessage(actionEvent: ActionEvent): Source[ProducerRecord[ActionEvent, ActionEvent], NotUsed] = {
    val record = new ProducerRecord[ActionEvent, ActionEvent](topic, actionEvent, actionEvent)
    Source.single(record).alsoTo(kafkaSink)
  }

  private implicit val entityStreaming: JsonEntityStreamingSupport = EntityStreamingSupport.json()

  def bindToPath(bindToPort: Int): Future[ServerBinding] = {

    import akka.http.scaladsl.server.Directives._
    import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
    val route: Route = path("") {
      post {
        decodeRequest {
          entity(as[ActionEvent]) { request =>
            println(request.toString)
            complete(publishMessage(request).map(_ => ""))
          }
        }
      }
    }

    println(s"Server going online at http://localhost:$bindToPort/")
    Http().bindAndHandle(route, interface = "localhost", port = bindToPort)
  }

  private val port                 = 9000
  val bound: Future[ServerBinding] = bindToPath(port)

  sys.addShutdownHook {
    println(s"Unbinding from port: ${port}...")
    println(Await.result(bound.flatMap(_.unbind()), 2.seconds))
  }

}
