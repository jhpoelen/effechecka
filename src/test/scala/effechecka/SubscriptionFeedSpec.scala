package effechecka

import java.net.URL
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
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

class SubscriptionFeedSpec extends TestKit(ActorSystem("KafkaIntegrationSpec"))
  with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach
  with ConversionCheckedTripleEquals with SubscriptionFeed with SubscriptionProtocols
  with SubscriptionsCassandra with Configure {

  implicit val materializer = ActorMaterializer()(system)
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
    session.execute("TRUNCATE effechecka.subscriptions")
    topic = "test-topic-" + uuid
    groupId = "test-group-" + uuid
    clientId = "test-client-" + uuid
  }

  val initialMsg = "some initial test message to establish topic"

  "subscribe" in {
    val event = SubscriptionEvent(OccurrenceSelector("taxa", "wkt", "traits"), new URL("mailto:foo@bar"), "subscribe")

    val subscriber: Probe[SubscriptionEvent] = createTopicAndSubscribe(event)
    val probe = TestSource.probe[SubscriptionEvent]
      .toMat(subscriptionHandler(topic))(Keep.left)
      .run()
    probe.sendNext(event)

    subscriber
      .request(1)
      .expectNext(event)

    subscriber.cancel()
  }

  "unsubscribe" in {
    val selector: OccurrenceSelector = OccurrenceSelector("taxa", "wkt", "traits")
    val event = SubscriptionEvent(selector, new URL("mailto:foo@bar"), "subscribe")
    val subscriber: Probe[SubscriptionEvent] = createTopicAndSubscribe(event)

    val probe = TestSource.probe[SubscriptionEvent]
      .toMat(subscriptionHandler(topic))(Keep.left)
      .run()
    probe.sendNext(event)

    subscriber
      .request(1)
      .expectNext(event)

    subscribersOf(selector) should contain(new URL("mailto:foo@bar"))

    val unsubscribeEvent = SubscriptionEvent(selector, new URL("mailto:foo@bar"), "unsubscribe")
    probe.sendNext(unsubscribeEvent)

    subscriber
      .request(1)
      .expectNext(unsubscribeEvent)

    subscribersOf(selector) shouldNot contain(new URL("mailto:foo@bar"))
    subscriber.cancel()
  }

  def createTopicAndSubscribe(event: SubscriptionEvent): TestSubscriber.Probe[SubscriptionEvent] = {
    val settings = ProducerSettings(system, new StringSerializer, new StringSerializer)
      .withBootstrapServers("localhost:9092")

    val producer = settings.createKafkaProducer()
    producer.send(new ProducerRecord(topic, initialMsg))
    producer.close(60, TimeUnit.SECONDS)

    val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer, Set(topic))
      .withGroupId(groupId)
      .withClientId(clientId)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .withBootstrapServers("localhost:9092")

    Consumer.plainSource(consumerSettings)
      .filterNot(_.value == initialMsg)
      .map(_.value.parseJson.convertTo[SubscriptionEvent])
      .runWith(TestSink.probe)
  }
}