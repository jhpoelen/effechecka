package effechecka

import org.scalatest.{Matchers, WordSpec}
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.http.scaladsl.server._
import Directives._
import akka.http.scaladsl.model.ContentType
import akka.http.scaladsl.model.ContentTypes
import akka.http.scaladsl.model.HttpCharset
import akka.http.scaladsl.model.HttpCharsets
import akka.http.scaladsl.model.MediaTypes

trait ChecklistFetcherStatic extends ChecklistFetcher {
  def itemsFor(checklist: ChecklistRequest): List[ChecklistItem] = List(ChecklistItem("donald", 1))
  def statusOf(checklist: ChecklistRequest): Option[String] = Some("ready")
  def request(checklist: ChecklistRequest): String = "requested"
}

trait OccurrenceCollectionFetcherStatic extends OccurrenceCollectionFetcher {
  val anOccurrence = Occurrence("Cartoona | mickey", 12.1, 32.1, 123L, 124L, "recordId", 456L, "archiveId")
  val aMonitor = OccurrenceMonitor(OccurrenceSelector("Cartoona | mickey", "some wkt string", "some trait selector"), "some status", 123)

  def occurrencesFor(checklist: OccurrenceCollectionRequest): List[Occurrence] = List(anOccurrence)
  def statusOf(checklist: OccurrenceCollectionRequest): Option[String] = Some("ready")
  def request(checklist: OccurrenceCollectionRequest): String = "requested"
  def monitors(): List[OccurrenceMonitor] = List(aMonitor)
}

class ChecklistService2Spec extends WordSpec with Matchers with ScalatestRouteTest with Service with ChecklistFetcherStatic with OccurrenceCollectionFetcherStatic {

  "The service" should {
    "return a 'ping' response for GET requests to /ping" in {
      Get("/ping") ~> route ~> check {
        responseAs[String] shouldEqual "pong"
      }
    }

    "return requested checklist" in {
      Get("/checklist?taxonSelector=Animalia,Insecta&wktString=ENVELOPE(-150,-50,40,10)") ~> route ~> check {
        responseAs[Checklist] shouldEqual Checklist(OccurrenceSelector("Animalia,Insecta", "ENVELOPE(-150,-50,40,10)",""), "ready", List(ChecklistItem("donald", 1)))
      }
    }

    "return requested occurrenceColection" in {
      Get("/occurrences?taxonSelector=Animalia,Insecta&wktString=ENVELOPE(-150,-50,40,10)") ~> route ~> check {
        responseAs[OccurrenceCollection] shouldEqual OccurrenceCollection(OccurrenceSelector("Animalia,Insecta", "ENVELOPE(-150,-50,40,10)",""), "ready", List(anOccurrence))
      }
    }

    "return requested monitors" in {
      Get("/monitors") ~> route ~> check {
        responseAs[List[OccurrenceMonitor]] should contain(OccurrenceMonitor(OccurrenceSelector("Cartoona | mickey", "some wkt string","some trait selector"), "some status", 123))
      }
    }

    "handle GET requests to other paths by returning instructive html" in {
      Get("/donald") ~> route ~> check {
        contentType shouldEqual ContentType(MediaTypes.`text/html`, HttpCharsets.`UTF-8`)
        responseAs[String] should include("/checklist?taxonSelector=Animalia,Insecta&amp;wktString=ENVELOPE(-150,-50,40,10)")
       
      }
    }
    
  }

}
