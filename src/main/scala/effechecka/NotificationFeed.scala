package effechecka

import akka.NotUsed
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.stream.FlowShape
import akka.stream.scaladsl.{Flow, GraphDSL, Merge, Source}

object NotificationFeed {
  def pushToClient(src: Source[String, Any]): Flow[Message, Message, NotUsed] = {
    Flow.fromGraph(GraphDSL.create() {
      implicit builder => {

        import GraphDSL.Implicits._

        val merge = builder.add(Merge[String](inputPorts = 2))
        val filter = builder.add(Flow[String].filter(_ => false))

        val fromClient = builder.add(Flow[Message].map[String] { msg => "nothing" })
        val toClient = builder.add(Flow[String].map[Message](x => TextMessage.Strict(x)))

        val log = builder.add(Flow[String].map[String](x => {
          println(s"sending: $x"); x
        }))

        val sourceFeed = builder.add(src)

        fromClient ~> filter ~> merge
        sourceFeed ~> log ~> merge ~> toClient

        FlowShape(fromClient.in, toClient.out)
      }
    })
  }
}
