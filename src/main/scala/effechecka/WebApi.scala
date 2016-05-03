package effechecka

import java.net.URL

import akka.NotUsed
import akka.event.{LoggingAdapter, Logging}
import akka.util.ByteString
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import akka.http.scaladsl.{server, Http}
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import org.joda.time.format.ISODateTimeFormat
import spray.json._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._
import akka.stream.scaladsl.{Concat, Keep, Source, Flow}
import akka.http.scaladsl.server.Directive0

case class MonitorStatus(selector: OccurrenceSelector, status: String, percentComplete: Double, eta: Long)


trait Protocols extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val occurrenceSelector = jsonFormat3(OccurrenceSelector)
  implicit val monitorStatusFormat = jsonFormat4(MonitorStatus)

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
  with SubscriptionFeed
  with NotificationFeedSourceKafka {

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
        } ~ path("occurrences.csv") {
          handleOccurrencesCsv
        } ~ path("occurrences") {
          handleOccurrences
        } ~ path("subscribe") {
          get {
            selectorParams.as(OccurrenceSelector) { ocSelector => {
              parameters('subscriber.as[String]) { subscriber =>
                complete {
                  handleSubscriptionEvent(ocSelector, new URL(subscriber), "subscribe")
                  s"subscribed [$subscriber]"
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
                  handleSubscriptionEvent(ocSelector, new URL(subscriber), "unsubscribe")
                  s"unsubscribed [$subscriber]"
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
                OccurrenceCollection(ocSelector, Option(status), List())
              }
            }
            }
          }
        } ~ path("notify") {
          get {
            selectorParams.as(OccurrenceSelector) { ocSelector => {
              parameters('addedBefore.as[String] ?, 'addedAfter.as[String] ?) { (addedBefore, addedAfter) =>
                val ocRequest = OccurrenceCollectionRequest(ocSelector, Some(1), addedBefore, addedAfter)
                if (occurrencesFor(ocRequest).hasNext) {
                  val subscribers = subscribersOf(ocSelector)
                  for (subscriber <- subscribers) {
                    handleSubscriptionEvent(SubscriptionEvent(ocSelector, subscriber, "notify", addedBefore, addedAfter))
                  }
                  complete {
                    "change detected: sent notifications"
                  }
                } else {
                  complete {
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

  def handleOccurrences: server.Route = {
    get {
      selectorParams.as(OccurrenceSelector) { ocSelector => {
        parameters('addedBefore.as[String] ?, 'addedAfter.as[String] ?) { (addedBefore, addedAfter) =>
          parameters('limit.as[Int] ? 20) { limit =>
            val ocRequest = OccurrenceCollectionRequest(ocSelector, Some(limit), addedBefore, addedAfter)
            val statusOpt: Option[String] = statusOf(ocSelector)
            complete {
              statusOpt match {
                case Some("ready") => {
                  OccurrenceCollection(ocSelector, Some("ready"), occurrencesFor(ocRequest).toList)
                }
                case None =>
                  OccurrenceCollection(ocSelector, Some(request(ocSelector)))
                case _ =>
                  OccurrenceCollection(ocSelector, statusOpt)
              }
            }
          }
        }
      }
      }
    }
  }

  def handleOccurrencesCsv: server.Route = {
    get {
      selectorParams.as(OccurrenceSelector) { ocSelector => {
        parameters('addedBefore.as[String] ?, 'addedAfter.as[String] ?) { (addedBefore, addedAfter) =>
          parameters('limit.as[Int] ? 20) { limit =>
            val ocRequest = OccurrenceCollectionRequest(selector = ocSelector, limit = None, addedBefore = addedBefore, addedAfter = addedAfter)
            val statusOpt: Option[String] = statusOf(ocSelector)
            statusOpt match {
              case Some("ready") => {
                encodeResponse {
                  complete {
                    val occurrenceSource = Source.fromIterator[ByteString]({ () => occurrencesFor(ocRequest)
                      .map(occurrence => {
                        ByteString(CsvUtils.toOccurrenceRow(occurrence))
                      })
                    })
                    val header = Source.single[ByteString](ByteString("taxon name,taxon path,lat,lng,eventStartDate,occurrenceId,firstAddedDate,source\n"))
                    HttpEntity(ContentTypes.`text/csv(UTF-8)`, Source.combine(header, occurrenceSource)(Concat[ByteString]))
                  }
                }
              }
              case _ => complete {
                StatusCodes.NotFound
              }
            }

          }
        }
      }
      }
    }
  }

  def handleSubscriptionEvent(ocSelector: OccurrenceSelector, subscriber: URL, action: String): NotUsed = {
    handleSubscriptionEvent(SubscriptionEvent(ocSelector, subscriber, action))
  }

  def handleSubscriptionEvent(event: SubscriptionEvent): NotUsed = {
    Source.single(event)
      .to(subscriptionHandler("effechecka-subscription")).run()
  }
}

object WebApi extends App with Service with Configure
  with SubscriptionsCassandra
  with ChecklistFetcherCassandra
  with OccurrenceCollectionFetcherCassandra {
  implicit val system = ActorSystem("effechecka")
  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  val logger = Logging(system, getClass)

  Http().bindAndHandle(route, config.getString("effechecka.host"), config.getInt("effechecka.port"))
}
