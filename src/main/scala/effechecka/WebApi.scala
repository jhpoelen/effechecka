package effechecka

import java.util.UUID

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.ContentType.WithCharset
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive0, Directive1, Route, ValidationRejection}
import akka.http.scaladsl.{Http, server}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Concat, Source}
import akka.util.ByteString
import org.effechecka.selector.{DateTimeSelector, OccurrenceSelector, UuidUtils}
import spray.json._

trait Protocols extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val occurrenceSelector = jsonFormat4(OccurrenceSelector)

  implicit val checklistFormat = jsonFormat2(ChecklistRequest)
  implicit val itemFormat = jsonFormat2(ChecklistItem)
  implicit val checklist2Format = jsonFormat3(Checklist)

  implicit val dateTimeSelectorFormat = jsonFormat2(DateTimeSelector)
  implicit val occurrenceRequestFormat = jsonFormat3(OccurrenceRequest)
  implicit val occurrenceFormat = jsonFormat8(Occurrence)
  implicit val occurrenceCollection2Format = jsonFormat3(OccurrenceCollection)
  implicit val occurrenceMonitorFormat = jsonFormat3(OccurrenceMonitor)
}


trait Service extends Protocols
  with ChecklistFetcher
  with Fetcher
  with SelectorRegistry
  with SelectorValidator
  with OccurrenceCollectionFetcher {

  private def addAccessControlHeaders: Directive0 = {
    mapResponseHeaders { headers =>
      `Access-Control-Allow-Origin`.* +: headers
    }
  }



  val selectorValueParams: Directive1[OccurrenceSelector] = {
    parameters('taxonSelector.as[String] ? "", 'wktString.as[String], 'traitSelector.as[String] ? "").tflatMap {
      case (taxon: String, wkt: String, traits: String) => {
        val selector = OccurrenceSelector(taxonSelector = normalizeSelector(taxon),
          wktString = wkt,
          traitSelector = normalizeSelector(traits))
        if (valid(selector)) {
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
        } ~ parameters('source.as[String]) { source =>
          usageRoutes(source)
        } ~ path("monitors") {
          get {
            complete {
              monitors()
            }
          }
        } ~ path("ping") {
          complete("pong")
        } ~ path("scrub") {
          val unregisteredSelectors = unregisterSelectors((selector: OccurrenceSelector) => invalid(selector)).mkString("unregistered invalid selectors: [", ",", "]")
          complete(unregisteredSelectors)
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
      addedParams.as(DateTimeSelector) { added =>
        redirect("http://gimmefreshdata.github.io/?" + UuidUtils.queryParamsFor(ocSelector, added), StatusCodes.TemporaryRedirect)
      }
    } ~ path("checklist") {
      get {
        parameters('limit.as[Int] ? 20) { limit =>
          val checklist = ChecklistRequest(ocSelector, limit)
          val statusOpt: Option[String] = statusOf(checklist)
          val (items, status) = statusOpt match {
            case Some("ready") => (itemsFor(checklist), "ready")
            case None => {
              (Iterator(), request(checklist))
            }
            case _ => (Iterator(), statusOpt.get)
          }
          complete {
            Checklist(ocSelector, status, items.toList)
          }
        }
      }
    } ~ path("occurrences.tsv") {
      handleOccurrencesTsv(ocSelector)
    } ~ path("occurrences") {
      handleOccurrences(ocSelector)
    } ~ path("update") {
      get {
        complete {
          val status = request(ocSelector)
          OccurrenceCollection(ocSelector, Option(status), List())
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
              val ocRequest = OccurrenceRequest(ocSelector, Some(limit), added)
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
              val ocRequest = OccurrenceRequest(selector = ocSelector, limit = limit, added)
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


}

object WebApi extends App with Service with Configure
  with SelectorRegistryCassandra
  with ChecklistFetcherCassandra
  with OccurrenceCollectionFetcherCassandra {
  implicit val system = ActorSystem("effechecka")
  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  val logger = Logging(system, getClass)

  Http().bindAndHandle(route, config.getString("effechecka.host"), config.getInt("effechecka.port"))
}
