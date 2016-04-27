package effechecka

import java.net.URL
import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.http.scaladsl.server.Directives
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.{Source, Sink}
import akka.stream.testkit.TestPublisher.Probe
import akka.stream.testkit.scaladsl.TestSource
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.{Matchers, WordSpec}


class SubscriberFeedKafkaSpec extends WordSpec with Matchers
  with Directives with ScalatestRouteTest with SubscriberFeed {

  "send record to topic" in {
    val settings = ProducerSettings(system, new StringSerializer, new StringSerializer)
      .withBootstrapServers("localhost:9092")

    val producer = settings.createKafkaProducer()
    producer.send(new ProducerRecord("effechecka-subscription", "some test message"))
    producer.close(60, TimeUnit.SECONDS)

    val event = SelectorSubscriptionEvent(OccurrenceSelector("taxa", "wkt", "traits"), new URL("mailto:foo@bar"))

    val probe: Source[ProducerRecord[String, String], Probe[ProducerRecord[String, String]]] = TestSource.probe[ProducerRecord[String, String]]
    probe.runWith(Producer.plainSink(settings))


  }
}