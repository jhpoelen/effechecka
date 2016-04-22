package effechecka

import java.util.UUID

import akka.actor.ActorSystem
import akka.kafka.ConsumerSettings
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.scaladsl.Source
import org.apache.kafka.common.serialization.StringDeserializer

trait NotificationFeedSource[T,M] {
  def feed: Source[T, M]
}

trait NotificationFeedSourceKafka extends NotificationFeedSource[String, Control] {

  implicit val system: ActorSystem

  def feed: Source[String, Control] = {
    val settings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer,
      Set("effechecka-selector"))
      .withGroupId(UUID.randomUUID().toString)
      .withBootstrapServers("localhost:9092")

    Consumer.plainSource(settings).map(record => record.value())
  }
}


