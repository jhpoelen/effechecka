package effechecka

import akka.event.{ LoggingAdapter, Logging }
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.xml.ScalaXmlSupport._
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import spray.json._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.model.HttpHeader
import akka.http.scaladsl.model.ws.{ Message, TextMessage }
import akka.stream.scaladsl.Flow
import akka.http.scaladsl.server.Directive0
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.StatusCode
import akka.http.scaladsl.model.MediaTypes

trait Protocols extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val checklistFormat = jsonFormat4(ChecklistRequest)
  implicit val itemFormat = jsonFormat2(ChecklistItem)
  implicit val checklist2Format = jsonFormat5(Checklist)

  implicit val occurrenceRequestFormat = jsonFormat4(OccurrenceCollectionRequest)
  implicit val occurrenceFormat = jsonFormat7(Occurrence)
  implicit val occurrenceCollection2Format = jsonFormat5(OccurrenceCollection)
}


trait Service extends Protocols with ChecklistFetcher with OccurrenceCollectionFetcher {
  
  private def addAccessControlHeaders: Directive0 = {
    mapResponseHeaders { headers =>
      `Access-Control-Allow-Origin`.* +: headers
    }
  }

  val echoService: Flow[Message, Message, _] = Flow[Message].map {
    case TextMessage.Strict(txt) => TextMessage("ECHO: " + txt)
    case _                       => TextMessage("Message type unsupported")
  }

  val route =
    logRequestResult("checklist-service") {
      addAccessControlHeaders {
        path("checklist") {
          get {
            parameters('taxonSelector.as[String], 'wktString.as[String], 'traitSelector.as[String] ? "", 'limit.as[Int] ? 20).as(ChecklistRequest) { checklist =>
              val statusOpt: Option[String] = statusOf(checklist)
              val (items, status) = statusOpt match {
                case Some("ready") => (itemsFor(checklist), "ready")
                case None => (List(), request(checklist))
                case _ => (List(), statusOpt.get)
              }
              complete {
                Checklist(checklist.taxonSelector, checklist.wktString, checklist.traitSelector, status, items)
              }
            }
          }
        } ~ path("occurrenceCollection") {
          get {
            parameters('taxonSelector.as[String], 'wktString.as[String], 'traitSelector.as[String] ? "", 'limit.as[Int] ? 20).as(OccurrenceCollectionRequest) { ocRequest =>
              val statusOpt: Option[String] = statusOf(ocRequest)
              val (items, status) = statusOpt match {
                case Some("ready") => (occurrencesFor(ocRequest), "ready")
                case None => (List(), request(ocRequest))
                case _ => (List(), statusOpt.get)
              }
              complete {
                OccurrenceCollection(ocRequest.taxonSelector, ocRequest.wktString, ocRequest.traitSelector, status, items)
              }
            }
          }
        } ~ path("ws-echo") {
          get {
            handleWebsocketMessages(echoService)
          }
        } ~ path("ping") {
          complete("pong")
        } ~ get {
          getFromResource("web/index.html")
        }
      }
    }
}

object Main extends App with Service with Configure with ChecklistFetcherCassandra with OccurrenceCollectionFetcherCassandra {
  implicit val system = ActorSystem("effechecka")
  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher
  
  val logger = Logging(system, getClass)

  Http().bindAndHandle(route, config.getString("effechecka.host"), config.getInt("effechecka.port"))
}
