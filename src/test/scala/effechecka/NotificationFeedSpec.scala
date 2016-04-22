package effechecka

import akka.NotUsed

import scala.concurrent.duration._

import akka.util.ByteString

import akka.stream.{FlowShape, OverflowStrategy}
import akka.stream.scaladsl._

import akka.http.scaladsl.model.ws.{TextMessage, Message, BinaryMessage}
import akka.http.scaladsl.testkit.WSProbe
import akka.http.scaladsl.server.Directives
import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.{Matchers, WordSpec}




trait NotificationFeedSourceStatic extends NotificationFeedSource[String, NotUsed]  {
  def feed = {
    Source.single("Welcome!")
  }
}

class NotificationFeedSpec extends WordSpec with Matchers
  with Directives with ScalatestRouteTest with NotificationFeedSourceStatic {

  "sayHiService" in {
    def selectorFeed: Flow[Message, Message, Any] =
      Flow[Message].mapConcat {
        case tm: TextMessage ⇒
          TextMessage(Source.single("Hi ") ++ tm.textStream ++ Source.single("!")) :: Nil
        case bm: BinaryMessage ⇒
          // ignore binary messages but drain content to avoid the stream being clogged
          bm.dataStream.runWith(Sink.ignore)
          Nil
      }
    val websocketRoute =
      path("sayHi") {
        handleWebSocketMessages(selectorFeed)
      }

    val wsClient = WSProbe()

    WS("/sayHi", wsClient.flow) ~> websocketRoute ~>
      check {
        // check response for WS Upgrade headers
        isWebSocketUpgrade shouldEqual true

        // manually run a WS conversation
        wsClient.sendMessage("Peter")
        wsClient.expectMessage("Hi Peter!")

        wsClient.sendMessage(BinaryMessage(ByteString("abcdef")))
        wsClient.expectNoMessage(100.millis)

        wsClient.sendMessage("John")
        wsClient.expectMessage("Hi John!")

        wsClient.sendCompletion()
        wsClient.expectCompletion()
      }
  }

  "welcome on connect" in {

    val websocketRoute =
      path("welcome") {
        handleWebSocketMessages(NotificationFeed.pushToClient(feed))
      }

    val anotherClient = WSProbe()

    WS("/welcome", anotherClient.flow) ~> websocketRoute ~>
      check {
        // check response for WS Upgrade headers
        isWebSocketUpgrade shouldEqual true

        // check pro-active welcome message
        anotherClient.expectMessage("Welcome!")

        anotherClient.sendMessage(BinaryMessage(ByteString("abcdef")))
        anotherClient.expectNoMessage(100.millis)

        anotherClient.sendMessage("John")
        anotherClient.expectNoMessage(100.millis)

        anotherClient.sendCompletion()
        anotherClient.expectCompletion()
      }
  }
}