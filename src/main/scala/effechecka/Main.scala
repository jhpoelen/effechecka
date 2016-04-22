package effechecka

import java.net.URL

import akka.event.{LoggingAdapter, Logging}
import akka.http.scaladsl.server.directives.ParameterDirectives.ParamDef
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.xml.ScalaXmlSupport._
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import spray.json._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.stream.scaladsl.Flow
import akka.http.scaladsl.server.Directive0

trait Protocols extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val occurrenceSelector = jsonFormat3(OccurrenceSelector)

  implicit val checklistFormat = jsonFormat2(ChecklistRequest)
  implicit val itemFormat = jsonFormat2(ChecklistItem)
  implicit val checklist2Format = jsonFormat3(Checklist)

  implicit val occurrenceRequestFormat = jsonFormat4(OccurrenceCollectionRequest)
  implicit val occurrenceFormat = jsonFormat8(Occurrence)
  implicit val occurrenceCollection2Format = jsonFormat3(OccurrenceCollection)
  implicit val occurrenceMonitorFormat = jsonFormat3(OccurrenceMonitor)
}


trait Service extends Protocols
  with ChecklistFetcher
  with OccurrenceCollectionFetcher
  with Subscriptions
  with NotificationFeedSourceKafka
  with NotificationSender {

  private def addAccessControlHeaders: Directive0 = {
    mapResponseHeaders { headers =>
      `Access-Control-Allow-Origin`.* +: headers
    }
  }


  val selectorParams = parameters('taxonSelector.as[String], 'wktString.as[String], 'traitSelector.as[String] ? "")

  val route =
    logRequestResult("checklist-service") {
      addAccessControlHeaders {
        path("checklist") {
          get {
            selectorParams.as(OccurrenceSelector) { ocSelector =>
              parameters('limit.as[Int] ? 20) { limit =>
                val checklist = ChecklistRequest(ocSelector, limit)
                val statusOpt: Option[String] = statusOf(checklist)
                val (items, status) = statusOpt match {
                  case Some("ready") => (itemsFor(checklist), "ready")
                  case None => (List(), request(checklist))
                  case _ => (List(), statusOpt.get)
                }
                complete {
                  Checklist(ocSelector, status, items)
                }
              }
            }
          }
        } ~ path("occurrences") {
          get {
            selectorParams.as(OccurrenceSelector) { ocSelector => {
              parameters('limit.as[Int] ? 20) { limit =>
                parameters('addedBefore.as[String] ?, 'addedAfter.as[String] ?) { (addedBefore, addedAfter) =>
                  val ocRequest = OccurrenceCollectionRequest(ocSelector, limit, addedBefore, addedAfter)
                  val statusOpt: Option[String] = statusOf(ocSelector)
                  val (items, status) = statusOpt match {
                    case Some("ready") => (occurrencesFor(ocRequest), "ready")
                    case None => (List(), request(ocSelector))
                    case _ => (List(), statusOpt.get)
                  }
                  complete {
                    OccurrenceCollection(ocSelector, status, items)
                  }
                }
              }
            }
            }
          }
        } ~ path("subscribe") {
          get {
            selectorParams.as(OccurrenceSelector) { ocSelector => {
              parameters('subscriber.as[String]) { subscriber =>
                complete {
                  subscribe(new URL(subscriber), ocSelector).toString
                }
              }
            }
            }
          }
        } ~ path("unsubscribe") {
          get {
            selectorParams.as(OccurrenceSelector) { ocSelector => {
              parameters('subscriber.as[String]) { subscriber =>
                complete {
                  unsubscribe(new URL(subscriber), ocSelector).toString
                }
              }
            }
            }
          }
        } ~ path("update") {
          get {
            selectorParams.as(OccurrenceSelector) { ocSelector => {
              complete {
                val status = request(ocSelector)
                OccurrenceCollection(ocSelector, status, List())
              }
            }
            }
          }
        } ~ path("notify") {
          get {
            selectorParams.as(OccurrenceSelector) { ocSelector => {
              parameters('addedBefore.as[String] ?, 'addedAfter.as[String] ?) { (addedBefore, addedAfter) =>
                val ocRequest = OccurrenceCollectionRequest(ocSelector, 1, addedBefore, addedAfter)
                val occurrences: List[Occurrence] = occurrencesFor(ocRequest)
                complete {
                  if (occurrences.nonEmpty) {
                    val subscribers = subscribersOf(ocSelector)
                    for (subscriber <- subscribers) {
                      sendNotification(subscriber, ocRequest)
                    }
                    "change detected: sent notifications"
                  } else {
                    "no change: did not send notifications"
                  }
                }
              }
            }
            }
          }
        } ~ (path("monitors") & selectorParams.as(OccurrenceSelector)) { (ocSelector) => {
          get {
            complete {
              monitorOf(ocSelector)
            }
          }
        }
        } ~ path("monitors") {
          get {
            complete {
              monitors()
            }
          }
        } ~ path("feed") {
          handleWebSocketMessages(NotificationFeed.pushToClient(feed))
        } ~ path("ping") {
          complete("pong")
        } ~ get {
          getFromResource("web/index.html")
        }
      }
    }
}

object Main extends App with Service with Configure
  with SubscriptionsCassandra
  with ChecklistFetcherCassandra
  with OccurrenceCollectionFetcherCassandra
  with NotificationSenderSendGrid {
  implicit val system = ActorSystem("effechecka")
  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  val logger = Logging(system, getClass)

  Http().bindAndHandle(route, config.getString("effechecka.host"), config.getInt("effechecka.port"))
}
