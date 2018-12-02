import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model.StatusCodes._
import akka.stream.ActorMaterializer
import io.circe.Json
import io.circe.generic.auto._
import io.circe.java8.time._
import models.{ActionEvent, Event}
import org.apache.spark.internal.Logging

import scala.concurrent.{ExecutionContext, Future}

case class EventServer(callback: Event => Unit) {

  private val log = org.apache.log4j.Logger.getLogger(this.getClass)

  def bindToPath(relativePath: String, bindToPort: Int)(implicit sys: ActorSystem,
                                                        ec: ExecutionContext,
                                                        mat: ActorMaterializer): Future[ServerBinding] = {

    import akka.http.scaladsl.server.Directives._
    import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
    val route = path(relativePath) {
      post {
        decodeRequest {
          entity(as[ActionEvent]) { request =>
            log.debug(request.toString)
            callback(request)
            complete {
              OK
            }
          }
        }
      }
    }

    log.debug(s"Server going online at http://localhost:$bindToPort/")
    Http().bindAndHandle(route, interface = "localhost", port = bindToPort)
  }

}
