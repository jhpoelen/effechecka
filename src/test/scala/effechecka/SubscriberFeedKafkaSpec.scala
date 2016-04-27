package effechecka

import java.net.URL
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.server.Directives
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Source, Sink}
import akka.stream.testkit.TestPublisher.Probe
import akka.stream.testkit.scaladsl.TestSource
import akka.testkit.TestKit
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.scalactic.ConversionCheckedTripleEquals
import org.scalatest._
import scala.concurrent.duration._

class SubscriberFeedKafkaSpec extends TestKit(ActorSystem("KafkaIntegrationSpec"))
  with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach
  with ConversionCheckedTripleEquals with SubscriberFeed {

  implicit val mat = ActorMaterializer()(system)
  implicit val ec = system.dispatcher

  def uuid = UUID.randomUUID().toString

  var topic: String = _

  override def afterAll(): Unit = {
    shutdown(system, 30.seconds)
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    topic = "test-topic-" + uuid
  }

  "send record to topic" in {
    val settings = ProducerSettings(system, new StringSerializer, new StringSerializer)
      .withBootstrapServers("localhost:9092")

    val producer = settings.createKafkaProducer()
    producer.send(new ProducerRecord(topic, "some test message"))
    producer.close(60, TimeUnit.SECONDS)

    val event = SelectorSubscriptionEvent(OccurrenceSelector("taxa", "wkt", "traits"), new URL("mailto:foo@bar"))

    val probe: Source[ProducerRecord[String, String], Probe[ProducerRecord[String, String]]] = TestSource.probe[ProducerRecord[String, String]]
    probe.runWith(Producer.plainSink(settings))


  }
}