package effechecka

import java.net.URL

import akka.NotUsed
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.stream.FlowShape
import akka.stream.scaladsl.{Flow, GraphDSL, Merge, Source}
import org.apache.kafka.clients.producer.ProducerRecord


case class SelectorSubscriptionEvent(selector: OccurrenceSelector, subscriber: URL)

trait SubscriberFeed {
  def feed: Flow[SelectorSubscriptionEvent, ProducerRecord[String, String], NotUsed] = {
    Flow.fromGraph(GraphDSL.create() {
      implicit builder => {

        import GraphDSL.Implicits._

        val fromSubscriber = builder.
          add(Flow[SelectorSubscriptionEvent].
            map[ProducerRecord[String, String]] {
            event => new ProducerRecord[String, String]("effechecka-subscription", "somevalue")
          })

        val log = builder.add(Flow[ProducerRecord[String, String]].map[ProducerRecord[String, String]](x => {
          println(s"adding: ${x.value()}")
          x
        }))

        fromSubscriber ~> log

        FlowShape(fromSubscriber.in, log.out)
      }
    })
  }
}
