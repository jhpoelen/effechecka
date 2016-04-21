package effechecka

import java.net.URL

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, HttpResponse, FormData}
import akka.stream.ActorMaterializer
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.Future
import scala.util.{Failure, Success}

class NotificationSenderSpec extends WordSpec with Matchers with ScalaFutures with NotificationSenderSendGrid {

  implicit val system = ActorSystem("notifier")
  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  "notification sender" should {
    "send a notification" in {
      val request = OccurrenceCollectionRequest(OccurrenceSelector("Insecta|Mammalia", "ENVELOPE(-150,-50,40,10)", "bodyMass greaterThan 2.7 kg"), 2)
      sendNotification(new URL("mailto:foo@bar"), request)
    }
  }
}