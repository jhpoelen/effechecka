package effechecka

import akka.actor.Actor
import org.apache.spark.deploy.SparkSubmit
import spray.http.HttpHeaders.RawHeader
import spray.routing._
import spray.http._
import MediaTypes._

import scala.util.parsing.json.{JSONObject, JSONArray}

// we don't implement our route structure directly in the service actor because
// we want to be able to test it independently, without having to spin up an actor
class ChecklistServiceActor extends Actor with ChecklistService {

  // the HttpService trait defines only one abstract member, which
  // connects the services environment to the enclosing actor or test
  def actorRefFactory = context

  // this actor only runs our route, but you could add
  // other things here, like request stream processing
  // or timeout handling
  def receive = runRoute(myRoute)
}


case class Checklist(taxonSelector: String, wktString: String, traitSelector: String, limit: Int)

// this trait defines our service behavior independently from the service actor
trait ChecklistService extends HttpService with ChecklistFetcher with Configure {

  val myRoute =
    path("checklist") {
      get {
        parameters('taxonSelector ?, 'wktString ?, 'traitSelector ? "", 'limit ? 20).as(Checklist) {
          request => {
            respondWithMediaType(`application/json`) {
              respondWithHeader(RawHeader("Access-Control-Allow-Origin", "*")) {
                val status: Option[String] = fetchChecklistStatus(request.taxonSelector, request.wktString, request.traitSelector)
                val (checklist_items, checklist_status) = status match {
                  case Some("ready") => (fetchChecklistItems(request.taxonSelector, request.wktString, request.traitSelector, request.limit), "ready")
                  case None =>
                    SparkSubmit.main(Array("--master", config.getString("effechecka.spark.master.url")
                      , "--class", "ChecklistGenerator"
                      , "--deploy-mode", "cluster"
                      , "--executor-memory", "32G"
                      , config.getString("effechecka.spark.job.jar")
                      , config.getString("effechecka.data.dir") + "*occurrence.txt"
                      , request.taxonSelector.replace(',', '|')
                      , request.wktString
                      , "cassandra"
                      , request.traitSelector
                      , config.getString("effechecka.data.dir") + "*traits.csv"
                    ))
                    (List(), insertChecklistRequest(request.taxonSelector, request.wktString, request.traitSelector))
                  case _ => (List(), status.get)
                }
                complete {
                  JSONObject(Map("taxonSelector" -> request.taxonSelector,
                    "wktString" -> request.wktString,
                    "traitSelector" -> request.traitSelector,
                    "status" -> checklist_status,
                    "items" -> JSONArray(checklist_items.map(JSONObject)))).toString()
                }
              }
            }
          }
        }
      }
    } ~ get {
      respondWithMediaType(`text/html`) {
        // XML is marshalled to `text/xml` by default, so we simply override here
        val url = """/checklist?taxonSelector=Animalia,Insecta&wktString=ENVELOPE(-150,-50,40,10)"""
        complete {
          <html>
            <body>
              <p>
                API for generating taxonomic checklists.
              </p>
              <p>
                example:
                <a href={url}>
                  {url}
                </a>
              </p>
            </body>
          </html>
        }
      }
    }
}