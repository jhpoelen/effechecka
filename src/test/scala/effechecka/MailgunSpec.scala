package effechecka

import java.net.URL

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes.Success
import akka.http.scaladsl.model.{HttpMethods, HttpRequest}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import org.scalactic.ConversionCheckedTripleEquals
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

class MailgunSpec extends TestKit(ActorSystem("MailgunIntegrationSpec"))
  with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach
  with ScalaFutures
  with ConversionCheckedTripleEquals with SubscriptionProtocols {

  implicit val materializer = ActorMaterializer()(system)
  implicit val ec = system.dispatcher
  implicit val defaultPatience = PatienceConfig(timeout = Span(10, Seconds), interval = Span(500, Millis))

  "send mailgun request" ignore {
    val someTo = "[enter email]"
    val apiKey = "[enter api key]"
    val anEmail = Email(to = someTo, subject = "hello!", text = "some text")
    val httpRequest: HttpRequest = EmailUtils.mailgunRequestFor(anEmail, apiKey)

    val futureResult = Http().singleRequest(httpRequest)
    whenReady(futureResult) { result =>
      result.status.intValue() should be(200)
    }
  }


}