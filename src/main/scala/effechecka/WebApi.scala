package effechecka

import java.net.URL
import java.util.UUID

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
import akka.http.scaladsl.server.{Route, Directive1, Directive0}

case class MonitorStatus(selector: OccurrenceSelector, status: String, percentComplete: Double, eta: Long)


trait Protocols extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val occurrenceSelector = jsonFormat3(OccurrenceSelector)
  implicit val monitorStatusFormat = jsonFormat4(MonitorStatus)

  implicit val checklistFormat = jsonFormat2(ChecklistRequest)
  implicit val itemFormat = jsonFormat2(ChecklistItem)
  implicit val checklist2Format = jsonFormat3(Checklist)

  implicit val dateTimeSelectorFormat = jsonFormat2(DateTimeSelector)
  implicit val occurrenceRequestFormat = jsonFormat3(OccurrenceCollectionRequest)
  implicit val occurrenceFormat = jsonFormat8(Occurrence)
  implicit val occurrenceCollection2Format = jsonFormat3(OccurrenceCollection)
  implicit val occurrenceMonitorFormat = jsonFormat3(OccurrenceMonitor)
}


trait Service extends Protocols
  with ChecklistFetcher
  with SelectorRegistry
  with OccurrenceCollectionFetcher
  with SubscriptionFeed
  with NotificationFeedSourceKafka {

  private def addAccessControlHeaders: Directive0 = {
    mapResponseHeaders { headers =>
      `Access-Control-Allow-Origin`.* +: headers
    }
  }

  val selectorValueParams: Directive1[OccurrenceSelector] = {
    parameters('taxonSelector.as[String], 'wktString.as[String], 'traitSelector.as[String] ? "").tflatMap {
      case (taxon: String, wkt: String, traits: String) => provide(OccurrenceSelector(taxon, wkt, traits))
      case _ => reject
    }
  }

  val uuidParams: Directive1[OccurrenceSelector] = {
    parameters('uuid.as[String]).flatMap {
      case (uuid: String) => selectorFor(UUID.fromString(uuid)) match {
        case Some(selector) => provide(selector)
        case None => reject
      }
      case _ => reject
    }
  }


  val selectorParams: Directive1[OccurrenceSelector] = {
    uuidParams | selectorValueParams
  }


  val route =
    logRequestResult("checklist-service") {
      addAccessControlHeaders {
        selectorParams { ocSelector =>
          registerSelector(ocSelector)
          selectorRoutes(ocSelector)
        } ~ parameters('source.as[String]) { source =>
          usageRoutes(source)
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

  def usageRoutes(source: String): Route = {
    path("monitoredOccurrences.csv") {
      handleMonitoredOccurrencesCsv(source)
    } ~ path("monitors" | " monitorsForOccurrence") {
      parameters('id.as[String]) { id =>
        get {
          complete {
            monitorsFor(source, id).toList
          }
        }
      }
    }
  }

  def selectorRoutes(ocSelector: OccurrenceSelector): Route = {
    path("checklist") {
      get {
        parameters('limit.as[Int] ? 20) { limit =>
          val checklist = ChecklistRequest(ocSelector, limit)
          val statusOpt: Option[String] = statusOf(checklist)
          val (items, status) = statusOpt match {
            case Some("ready") => (itemsFor(checklist), "ready")
            case None => {
              (List(), request(checklist))
            }
            case _ => (List(), statusOpt.get)
          }
          complete {
            Checklist(ocSelector, status, items)
          }
        }
      }
    } ~ path("occurrences.csv") {
      handleOccurrencesCsv(ocSelector)
    } ~ path("occurrences") {
      handleOccurrences(ocSelector)
    } ~ path("subscribe") {
      get {
        parameters('subscriber.as[String]) { subscriber =>
          complete {
            handleSubscriptionEvent(ocSelector, new URL(subscriber), "subscribe")
            s"subscribed [$subscriber]"
          }
        }
      }
    } ~ path("unsubscribe") {
      get {
        parameters('subscriber.as[String]) { subscriber =>
          complete {
            handleSubscriptionEvent(ocSelector, new URL(subscriber), "unsubscribe")
            s"unsubscribed [$subscriber]"
          }
        }
      }
    } ~ path("update") {
      get {
        complete {
          val status = request(ocSelector)
          OccurrenceCollection(ocSelector, Option(status), List())
        }
      }
    } ~ path("notify") {
      get {
        addedParams.as(DateTimeSelector) { added =>
          val ocRequest = OccurrenceCollectionRequest(ocSelector, Some(1), added)
          if (occurrencesFor(ocRequest).hasNext) {
            val subscribers = subscribersOf(ocSelector)
            for (subscriber <- subscribers) {
              handleSubscriptionEvent(SubscriptionEvent(ocSelector, subscriber, "notify", added.before, added.after))
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
    } ~ path("monitors") {
      get {
        complete {
          monitorOf(ocSelector)
        }
      }
    }
  }

  val addedParams = parameters('addedBefore.as[String] ?, 'addedAfter.as[String] ?)

  def handleOccurrences(ocSelector: OccurrenceSelector): server.Route = {
    get {
      addedParams.as(DateTimeSelector) {
        added =>
          parameters('limit.as[Int] ? 20) {
            limit =>
              val ocRequest = OccurrenceCollectionRequest(ocSelector, Some(limit), added)
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

  def handleOccurrencesCsv(ocSelector: OccurrenceSelector): server.Route = {
    get {
      addedParams.as(DateTimeSelector) {
        added =>
          parameters('limit.as[Int] ?) {
            limit =>
              val ocRequest = OccurrenceCollectionRequest(selector = ocSelector, limit = limit, added)
              val statusOpt: Option[String] = statusOf(ocSelector)
              statusOpt match {
                case Some("ready") => {
                  encodeResponse {
                    complete {
                      val occurrenceSource = Source.fromIterator[ByteString]({
                        () => occurrencesFor(ocRequest)
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

  def handleMonitoredOccurrencesCsv(source: String): server.Route = {
    get {
      parameters('source.as[String]) {
        source => {
          addedParams.as(DateTimeSelector) {
            added =>
              parameters('limit.as[Int] ?) {
                limit =>
                  encodeResponse {
                    complete {
                      val monitoredOccurrenceSource = Source.fromIterator[ByteString]({
                        () => monitoredOccurrencesFor(source, added, limit)
                          .map(occurrenceId => {
                            ByteString(s""""$occurrenceId"\n""")
                          })
                      })
                      val header = Source.single[ByteString](ByteString("occurrenceId\n"))
                      HttpEntity(ContentTypes.`text/csv(UTF-8)`, Source.combine(header, monitoredOccurrenceSource)(Concat[ByteString]))
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
  with SelectorRegistryCassandra
  with ChecklistFetcherCassandra
  with OccurrenceCollectionFetcherCassandra {
  implicit val system = ActorSystem("effechecka")
  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  val logger = Logging(system, getClass)

  Http().bindAndHandle(route, config.getString("effechecka.host"), config.getInt("effechecka.port"))
}
