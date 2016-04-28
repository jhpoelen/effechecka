package effechecka

import java.net.URL

import akka.NotUsed
import akka.actor.ActorSystem
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream._
import akka.stream.scaladsl._
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import spray.json._


case class SubscriptionEvent(selector: OccurrenceSelector, subscriber: URL, action: String)

trait SubscriptionProtocols extends Protocols {

  implicit object urlJsonFormat extends RootJsonFormat[URL] {
    def write(c: URL) =
      JsString(c.toString)

    def read(value: JsValue) = value match {
      case JsString(urlString) =>
        new URL(urlString)
      case _ => deserializationError("url expected")
    }
  }

  implicit val selectorSubscriptionEventFormat = jsonFormat3(SubscriptionEvent)
}


trait SubscriptionFeed extends Subscriptions with SubscriptionProtocols {
  implicit val system: ActorSystem
  implicit val materializer: ActorMaterializer

  lazy val settings = ProducerSettings(system, new StringSerializer, new StringSerializer)
    .withBootstrapServers("localhost:9092")
  lazy val kafkaSink: Sink[ProducerRecord[String, String], NotUsed] = Producer.plainSink(settings)


  def publishSubscriptionEvent(topic: String): Graph[SinkShape[SubscriptionEvent], Any] = {
    import GraphDSL.Implicits._
    GraphDSL.create() { implicit builder =>

      val flow = Flow[SubscriptionEvent].map(event => {
        new ProducerRecord(topic, null: String, event.toJson.toString)
      })

      val kafkaTopic: Inlet[ProducerRecord[String, String]] = builder.add(kafkaSink).in
      val eventToProducer = builder.add(flow)

      eventToProducer ~> kafkaTopic

      SinkShape(eventToProducer.in)
    }
  }

  def persistSubscription: Graph[FlowShape[SubscriptionEvent, SubscriptionEvent], Any] = {
    GraphDSL.create() { implicit builder =>
      val flow = Flow[SubscriptionEvent].map(event => {
        event.action match {
          case "subscribe" => subscribe(event.subscriber, event.selector)
          case "unsubscribe" => unsubscribe(event.subscriber, event.selector)
          case _ =>  //pass through
        }

        event
      })
      val flowShape = builder.add(flow)
      FlowShape(flowShape.in, flowShape.out)
    }
  }

  def subscriptionHandler(topic: String): Graph[SinkShape[SubscriptionEvent], NotUsed] = {

      import GraphDSL.Implicits._
      GraphDSL.create() { implicit builder =>

        val save = builder.add(persistSubscription)
        val notify = builder.add(publishSubscriptionEvent(topic))

        save ~> notify

        SinkShape(save.in)
      }
    }

}
