package effechecka

import java.net.URL
import java.util.UUID

import akka.NotUsed
import akka.event.{LoggingAdapter, Logging}
import akka.http.scaladsl.model.ContentType.WithCharset
import akka.util.ByteString
import akka.http.scaladsl.{server, Http}
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import org.locationtech.spatial4j.context.jts.JtsSpatialContext
import org.locationtech.spatial4j.io.WKTReader
import spray.json._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._
import akka.stream.scaladsl.{Concat, Keep, Source, Flow}
import akka.http.scaladsl.server.{ValidationRejection, Route, Directive1, Directive0}
import akka.http.scaladsl.model.StatusCode._

import scala.util.Try

case class MonitorStatus(selector: OccurrenceSelector, status: String, percentComplete: Double, eta: Long)


trait Protocols extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val occurrenceSelector = jsonFormat4(OccurrenceSelector)
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
  with Fetcher
  with SelectorRegistry
  with OccurrenceCollectionFetcher
  with SubscriptionFeed
  with NotificationFeedSourceKafka {

  private def addAccessControlHeaders: Directive0 = {
    mapResponseHeaders { headers =>
      `Access-Control-Allow-Origin`.* +: headers
    }
  }


  def isValidSelector(occurrence: OccurrenceSelector): Boolean = {
    Seq(validTaxonList _, validWktString _, validTraitSelector _).forall(_(occurrence))
  }

  def validWktString(occurrence: OccurrenceSelector): Boolean = {
    Try {
      new WKTReader(JtsSpatialContext.GEO, null).parse(occurrence.wktString)
    }.isSuccess
  }

  def validTaxonList(occurrence: OccurrenceSelector): Boolean = {
    occurrence.taxonSelector.matches("""[\w,|\s]+""")
  }

  def validTraitSelector(occurrence: OccurrenceSelector): Boolean = {
    def atLeastOneSupportedTrait: Boolean = {
      val traitMatcher = ".*(eventDate|source)+.*".r
      occurrence.traitSelector match {
        case traitMatcher(traits) => true
        case _ => false
      }

    }
    occurrence.traitSelector.trim.isEmpty || atLeastOneSupportedTrait
  }

  val selectorValueParams: Directive1[OccurrenceSelector] = {
    parameters('taxonSelector.as[String], 'wktString.as[String], 'traitSelector.as[String] ? "").tflatMap {
      case (taxon: String, wkt: String, traits: String) => {
        val selector = OccurrenceSelector(taxonSelector = normalizeSelector(taxon),
          wktString = wkt,
          traitSelector = normalizeSelector(traits))
        if (isValidSelector(selector)) {
          provide(selector)
        } else {
          reject(ValidationRejection("this always fails"))
        }
      }
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


  val selectorParameters: Directive1[OccurrenceSelector] = {
    uuidParams | selectorValueParams
  }


  val route =
    logRequestResult("checklist-service") {
      addAccessControlHeaders {
        selectorParameters { ocSelector =>
          parameters('ttlSeconds.as[Int] ?) { ttlSeconds =>
            registerSelector(ocSelector, ttlSeconds)
            selectorRoutes(ocSelector)
          }
        } ~ path("updateAll") {
          get {
            complete {
              requestAll()
            }
          }
        } ~ path("notifyAll") {
          get {
            addedParams.as(DateTimeSelector) { added =>
              val events = monitors().flatMap { monitor =>
                generateSubscriptionEventsFor(ocSelector = monitor.selector, added = added)
              }
              events.foreach(handleSubscriptionEvent)
              complete {
                val selectors = events.map(_.selector).distinct
                val selectorString = selectors.mkString("[", ":", "]")
                s"sent [${events.length}] notification${if (events.length > 1) "s" else ""} related occurrences added [$added] to monitors $selectorString"
              }
            }
          }
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
          complete(HttpResponse(status = StatusCodes.BadRequest))
        }
      }
    }


  def usageRoutes(source: String): Route = {
    path("monitoredOccurrences.tsv") {
      handleMonitoredOccurrencesTsv(source)
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
    path("view") {
      redirect("http://gimmefreshdata.github.io/?" + EmailUtils.queryParamsFor(ocSelector), StatusCodes.TemporaryRedirect)
    } ~ path("checklist") {
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
    } ~ path("occurrences.tsv") {
      handleOccurrencesTsv(ocSelector)
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
          val events = generateSubscriptionEventsFor(ocSelector, added)
          events.foreach(handleSubscriptionEvent)
          complete {
            val selectors = events.map(_.selector).distinct
            val selectorString = selectors.mkString("[", ":", "]")
            s"sent [${events.length}] notification${if (events.length > 1) "s" else ""} related occurrences added [$added] to monitors $selectorString"
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

  def generateSubscriptionEventsFor(ocSelector: OccurrenceSelector, added: DateTimeSelector): List[SubscriptionEvent] = {
    val ocRequest = OccurrenceCollectionRequest(ocSelector, Some(1), added)
    val events = if (occurrencesFor(ocRequest).hasNext) {
      subscribersOf(ocSelector).map(SubscriptionEvent(ocSelector, _, "notify", added.before, added.after))
    } else {
      List()
    }
    events
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

  private val contentType: WithCharset = MediaTypes.`text/tab-separated-values` withCharset HttpCharsets.`UTF-8`

  def handleOccurrencesTsv(ocSelector: OccurrenceSelector): server.Route = {
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
                      val header = Source.single[ByteString](ByteString(Seq("taxonName", "taxonPath", "lat", "lng", "eventStartDate", "occurrenceId", "firstAddedDate", "source", "occurrenceUrl").mkString("\t")))
                      HttpEntity(contentType, Source.combine(header, occurrenceSource)(Concat[ByteString]))
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

  def handleMonitoredOccurrencesTsv(source: String): server.Route = {
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
                            ByteString(s"\n$occurrenceId")
                          })
                      })
                      val header = Source.single[ByteString](ByteString("occurrenceId"))
                      HttpEntity(contentType, Source.combine(header, monitoredOccurrenceSource)(Concat[ByteString]))
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
