import java.nio.charset.StandardCharsets

import akka.actor.ActorSystem
import akka.http.javadsl.common.{EntityStreamingSupport, JsonEntityStreamingSupport}
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import akka.kafka.scaladsl.Producer
import akka.kafka.{ProducerMessage, ProducerSettings}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.ByteString
import akka.{Done, NotUsed}
import io.circe.generic.auto._
import io.circe.java8.time._
import models.{ActionEvent, _}
import org.apache.kafka.clients.producer.ProducerRecord
import io.circe.parser._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
object Server extends App {

  private implicit val system: ActorSystem          = ActorSystem("ingestion")
  private implicit val mat: ActorMaterializer       = ActorMaterializer()
  private implicit val ec: ExecutionContextExecutor = system.dispatcher
  private val bootstrapServers                      = "0.0.0.0:9092"
  private val topic                                 = "test"
  private val port                                  = 9000

  private val producerSettings: ProducerSettings[ActionEvent, ActionEvent] =
    ProducerSettings(system, keySerializer, valueSerializer)
      .withBootstrapServers(bootstrapServers)
      .withCloseTimeout(60.seconds)
      .withParallelism(16)
      .withDispatcher("akka.kafka.default-dispatcher")
      .withEosCommitInterval(100.millis)

  val justBind: Source[Http.IncomingConnection, Future[ServerBinding]] = Http().bind("localhost", port)

  val entityStreamingSupport: JsonEntityStreamingSupport = EntityStreamingSupport.json()

  val requestToEnvelope: Flow[HttpRequest, ProducerMessage.Envelope[ActionEvent, ActionEvent, HttpResponse], NotUsed] =
    Flow
      .apply[HttpRequest]
      .map(_.entity.dataBytes)
      .flatMapConcat(
        _.via(
          Flow[ByteString]
            .map(byteString => new String(byteString.toArray, StandardCharsets.UTF_8))
            .map(decode[ActionEvent])))
      .map(
        maybeActionEvent =>
          maybeActionEvent.fold(
            parsingError =>
              ProducerMessage.passThrough(HttpResponse(StatusCodes.BadRequest, entity = parsingError.toString)),
            actionEvent =>
              ProducerMessage.single(new ProducerRecord[ActionEvent, ActionEvent](topic, actionEvent, actionEvent),
                                     HttpResponse(StatusCodes.OK))
        )
      )

  private val kafkaFlow = Producer.flexiFlow[ActionEvent, ActionEvent, HttpResponse](producerSettings)
  private val resultFlow: Flow[HttpRequest, HttpResponse, NotUsed] =
    requestToEnvelope.via(kafkaFlow).map { _.passThrough }

  private val handleConnections: Sink[Http.IncomingConnection, Future[Done]] =
    Sink.foreach[Http.IncomingConnection] { connection =>
      val _ = connection.handleWith(resultFlow)
    }

  val eventualBinding = justBind.to(handleConnections).run()

  eventualBinding.foreach(binding => println(s"Server online at $port: $binding"))

  sys.addShutdownHook {
    println(s"Unbinding from port: $port...")
    println(Await.result(eventualBinding.flatMap(_.unbind()), 2.seconds))
  }

}
