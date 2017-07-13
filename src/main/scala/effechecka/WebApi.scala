package effechecka

import java.net.URL
import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive0, Directive1, Route, ValidationRejection}
import akka.http.scaladsl.{Http, server}
import akka.stream._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.effechecka.selector.DateTimeSelector
import spray.json._

case class SparkDispatchResponse(action: String,
                                 sparkServerVersion: Option[String] = None,
                                 success: Option[Boolean] = Some(false),
                                 message: Option[String] = None)

trait Protocols extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val responseParams = jsonFormat4(SparkDispatchResponse)
  implicit val selectorParams = jsonFormat3(SelectorParams)
  implicit val selectorParamsUUID = jsonFormat4(SelectorUUID)

  implicit object selectorJsonFormat extends RootJsonFormat[Selector] {
    def write(a: Selector) = a match {
      case p: SelectorParams => p.toJson
      case p: SelectorUUID => p.toJson
    }

    override def read(json: JsValue): Selector = {
      if (json.asJsObject.fields.contains("uuid")) {
        json.convertTo[SelectorUUID]
      } else json.convertTo[SelectorParams]
    }
  }

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
  with JobSubmitter
  with SelectorValidator
  with OccurrenceCollectionFetcher {

  private def addAccessControlHeaders: Directive0 = {
    mapResponseHeaders { headers =>
      `Access-Control-Allow-Origin`.* +: headers
    }
  }


  val selectorValueParams: Directive1[Selector] = {
    def normalizeSelector(taxonSelector: String) = {
      taxonSelector.replace(',', '|')
    }
    parameters('taxonSelector.as[String] ? "", 'wktString.as[String], 'traitSelector.as[String] ? "").tflatMap {
      case (taxon: String, wkt: String, traits: String) => {
        val selector = SelectorParams(taxonSelector = normalizeSelector(taxon),
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

  val uuidParams: Directive1[Selector] = {
    parameters('uuid.as[String]).flatMap {
      case (uuid: String) => provide(SelectorUUID(uuid = UUID.fromString(uuid).toString))
      case _ => reject
    }
  }

  val selectorParameters: Directive1[Selector] = {
    uuidParams | selectorValueParams
  }


  val route =
    logRequestResult("checklist-service") {
      addAccessControlHeaders {
        selectorParameters { ocSelector =>
          selectorRoutes(ocSelector)
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
        } ~ get {
          complete(HttpResponse(status = StatusCodes.BadRequest))
        }
      }
    }


  def usageRoutes(source: String): Route = {
    path("monitoredOccurrences.tsv") {
      handleMonitoredOccurrencesTsv(source)
    }
  }

  private val occurrencecollectiongenerator = "OccurrenceCollectionGenerator"
  private val checklistgenerator = "ChecklistGenerator"

  def selectorRoutes(ocSelector: Selector): Route = {
    path("checklist") {
      handleChecklistSummary(ocSelector)
    } ~ path("checklist.tsv") {
      handleChecklistTsv(ocSelector)
    } ~ path("occurrences") {
      handleOccurrences(ocSelector)
    } ~ path("occurrences.tsv") {
      handleOccurrencesTsv(ocSelector)
    } ~ path("monitors") {
      get {
        complete {
          monitorOf(ocSelector)
        }
      }
    }

  }

  private def handleChecklistTsv(ocSelector: Selector) = {
    get {
      parameters('limit.as[Int] ?) { limit =>
        val checklist = ChecklistRequest(ocSelector, limit)
        val statusOpt: Option[String] = statusOf(checklist)
        statusOpt match {
          case Some("ready") =>
            encodeResponse {
              complete {
                HttpEntity(tsvContentType, tsvFor(checklist))
              }
            }
          case _ =>
            submitIfPossible(checklist.selector, checklistgenerator)
        }
      }
    }
  }

  private def submitIfPossible(selector: Selector, sparkJobMainClass: String) = {
    complete {
      selector match {
        case s: SelectorParams => {
          val response = submit(s, sparkJobMainClass)
          if (response.success.getOrElse(false)) {
            StatusCodes.Processing
          } else {
            StatusCodes.BadRequest
          }
        }
        case _ =>
          StatusCodes.BadRequest
      }
    }
  }

  private def handleChecklistSummary(ocSelector: Selector) = {
    get {
      val checklist = ChecklistRequest(ocSelector, Some(20))
      val statusOpt: Option[String] = statusOf(checklist)
      complete {
        statusOpt match {
          case Some("ready") =>
            Checklist(ocSelector.withUUID(), "ready", itemsFor(checklist).toList)
          case _ =>
            checklist.selector match {
              case s: SelectorParams =>
                val msg = replyForSubmission(s, checklistgenerator)
                Checklist(s.withUUID(), msg, List.empty)
              case _ => StatusCodes.BadRequest
            }
        }
      }
    }
  }

  private def replyForSubmission(s: SelectorParams, jobName: String): String = {
    val resp = submit(s, jobName)
    (resp.success, resp.message) match {
      case (Some(false), Some("Already reached maximum submission size")) => "busy"
      case (Some(true), _) => "submitted"
      case (_, Some(msg)) => msg
      case (_, _) => "unknown"
    }
  }

  val addedParams = parameters('addedBefore.as[String] ?, 'addedAfter.as[String] ?)


  def handleOccurrences(ocSelector: Selector): server.Route = {
    get {
      addedParams.as(DateTimeSelector) {
        added =>
          val ocRequest = OccurrenceRequest(ocSelector, Some(20), added)
          val statusOpt: Option[String] = statusOf(ocSelector)
          complete {
            statusOpt match {
              case Some("ready") =>
                OccurrenceCollection(ocSelector.withUUID(), Some("ready"), occurrencesFor(ocRequest).toList)
              case None =>
                ocSelector match {
                  case s: SelectorParams => {
                    val msg = replyForSubmission(s, occurrencecollectiongenerator)
                    OccurrenceCollection(ocSelector.withUUID(), Some(msg))
                  }
                  case _ => StatusCodes.BadRequest
                }
              case _ =>
                OccurrenceCollection(ocSelector.withUUID(), statusOpt)
            }
          }
      }
    }
  }

  private val tsvContentType = MediaTypes.`text/tab-separated-values`.withCharset(HttpCharsets.`UTF-8`)

  def handleOccurrencesTsv(ocSelector: Selector): server.Route = {
    get {
      addedParams.as(DateTimeSelector) {
        added =>
          parameters('limit.as[Int] ?) {
            limit =>
              val ocRequest = OccurrenceRequest(selector = ocSelector, limit = limit, added)
              val statusOpt: Option[String] = statusOf(ocSelector)
              statusOpt match {
                case Some("ready") =>
                  encodeResponse {
                    complete {
                      HttpEntity(tsvContentType, occurrencesTsvFor(ocRequest))
                    }
                  }
                case _ =>
                  submitIfPossible(ocSelector, occurrencecollectiongenerator)
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
                      HttpEntity(tsvContentType, monitoredOccurrencesFor(source, added, limit))
                    }
                  }
              }
          }
        }
      }
    }
  }

}


object WebApi extends App with Service
  with Configure
  with SparkSubmitter
  with ChecklistFetcherHDFS
  with OccurrenceCollectionFetcherHDFS {

  implicit val system = ActorSystem("effechecka")

  val decider: Supervision.Decider = { e =>
    logger.error("Unhandled exception in stream", e)
    Supervision.Stop
  }
  implicit val materializerSettings = ActorMaterializerSettings(system).withSupervisionStrategy(decider)
  implicit val materializer = ActorMaterializer(materializerSettings)(system)
  implicit val ec = system.dispatcher

  implicit val configHadoop: Configuration =
    sys.env.get("HADOOP_CONF_DIR") match {
      case Some(confDir) =>
        logger.info(s"attempting to override configuration in [$confDir]")
        val conf = new Configuration()
        conf.addResource(new URL(s"file:///$confDir/hdfs-site.xml"))
        conf.addResource(new URL(s"file:///$confDir/core-site.xml"))
        conf
      case _ =>
        new Configuration()
    }

  implicit val fs: FileSystem = FileSystem.get(configHadoop)


  logger.info("--- environment variable start ---")
  sys.env.foreach(env => logger.info(env.toString()))
  logger.info("--- environment variable end ---")

  val iterator = configHadoop.iterator
  logger.info("--- hadoop config start ---")
  while (iterator.hasNext) {
    logger.info(iterator.next().toString)
  }
  logger.info("--- hadoop config end ---")

  Http().bindAndHandle(route, config.getString("effechecka.host"), config.getInt("effechecka.port"))
}
