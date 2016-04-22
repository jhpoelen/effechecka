package effechecka

import java.util.UUID

import akka.NotUsed
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer.Control
import scala.concurrent.duration._

import akka.kafka.scaladsl._
import akka.kafka.ConsumerSettings
import akka.stream.{FlowShape, ActorMaterializer}

import akka.stream.scaladsl._
import akka.util.ByteString
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.clients.consumer.ConsumerConfig


import akka.http.scaladsl.model.ws.{BinaryMessage, TextMessage, Message}
import akka.http.scaladsl.server.Directives
import akka.http.scaladsl.testkit.{WSProbe, ScalatestRouteTest}
import org.scalatest.{Matchers, WordSpec}






class NotificationFeedKafkaSpec extends WordSpec with Matchers
  with Directives with ScalatestRouteTest with NotificationFeedSourceKafka {

   "someting on connect" ignore {

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