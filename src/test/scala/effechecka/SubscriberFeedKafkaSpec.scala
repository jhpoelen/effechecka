package effechecka

import java.net.URL
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.server.Directives
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.kafka.{ConsumerSettings, ProducerSettings}
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Source, Sink}
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.testkit.TestPublisher.Probe
import akka.stream.testkit.scaladsl.TestSource
import akka.testkit.TestKit
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerConfig}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
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
  var groupId: String = _
  var clientId: String = _

  override def afterAll(): Unit = {
    shutdown(system, 30.seconds)
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    topic = "test-topic-" + uuid
    groupId = "test-group-" + uuid
    clientId = "test-client-" + uuid
  }

  val initialMsg = "some initial test message"

  "send record to topic" in {
    val settings = ProducerSettings(system, new StringSerializer, new StringSerializer)
      .withBootstrapServers("localhost:9092")

    val producer = settings.createKafkaProducer()
    producer.send(new ProducerRecord(topic, initialMsg))
    producer.close(60, TimeUnit.SECONDS)

    val event = SelectorSubscriptionEvent(OccurrenceSelector("taxa", "wkt", "traits"), new URL("mailto:foo@bar"))

    val testSource: Source[ProducerRecord[String, String], Probe[ProducerRecord[String, String]]] = TestSource.probe[ProducerRecord[String, String]]
    val (probe, notUsed) = testSource.toMat(Producer.plainSink(settings))(Keep.both).run()

    val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer, Set(topic))
      .withGroupId(groupId)
      .withClientId(clientId)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .withBootstrapServers("localhost:9092")

    val subscriber = Consumer.plainSource(consumerSettings)
      .filterNot(_.value == initialMsg)
      .map(_.value)
      .runWith(TestSink.probe)

    probe.sendNext(new ProducerRecord(topic, "some other test message"))

    subscriber
      .request(1)
      .expectNext("some other test message")

    subscriber.cancel()
  }
}