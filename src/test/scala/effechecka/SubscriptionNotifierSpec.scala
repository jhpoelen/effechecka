package effechecka

import java.net.URL
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.{BasicHttpCredentials, Authorization}
import akka.http.scaladsl.model.{Uri, FormData, HttpMethods, HttpRequest}
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, ProducerSettings}
import akka.stream._
import akka.stream.scaladsl._
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.TestSubscriber.Probe
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.TestKit
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.scalactic.ConversionCheckedTripleEquals
import org.scalatest._
import spray.json._

import scala.concurrent.duration._

class SubscriptionNotifierSpec extends TestKit(ActorSystem("StreamIntegrationSpec"))
  with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach
  with ConversionCheckedTripleEquals with Configure {

  implicit val materializer = ActorMaterializer()(system)
  implicit val ec = system.dispatcher


  "notify using mailgun" in {
    val event = SubscriptionEvent(OccurrenceSelector("taxa", "wkt", "traits"), new URL("mailto:foo@bar"), "notify")

    val actualRequest: HttpRequest = requestFor(event)
    actualRequest.uri should be(Uri("https://api.mailgun.net/v3/effechecka.org/messages"))
    actualRequest.method should be(HttpMethods.POST)

  }

  def requestFor(event: SubscriptionEvent): HttpRequest = {
    val (pub, sub) = TestSource.probe[SubscriptionEvent]
      .via(SubscriptionNotifier.subscriberEventToNotification())
      .toMat(TestSink.probe[HttpRequest])(Keep.both)
      .run()

    sub.request(n = 1)
    pub.sendNext(event)
    sub.requestNext()
  }

  "notify using webhook" in {
    val selector: OccurrenceSelector = OccurrenceSelector("taxa", "wkt", "traits")
    val event = SubscriptionEvent(selector, new URL("http://some.site"), "notify")
    val actualRequest: HttpRequest = requestFor(event)
    actualRequest.uri should be(Uri(s"http://some.site?uuid=${UuidUtils.uuidFor(selector)}"))
    actualRequest.method should be(HttpMethods.GET)
  }

}