package effechecka

import java.net.URL
import java.util.UUID


import akka.http.scaladsl.model.TransferEncodings.{gzip, deflate}
import akka.http.scaladsl.model.headers.{Location, `Accept-Encoding`}
import org.scalatest.{Matchers, WordSpec}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.http.scaladsl.model._

trait ChecklistFetcherStatic extends ChecklistFetcher {
  def itemsFor(checklist: ChecklistRequest): List[ChecklistItem] = List(ChecklistItem("donald", 1))

  def statusOf(checklist: ChecklistRequest): Option[String] = Some("ready")

  def request(checklist: ChecklistRequest): String = "requested"
}

trait SelectorRegistryStatic extends SelectorRegistry {

  def registerSelector(selector: OccurrenceSelector): UUID = UUID.fromString("c7483fed-ff5c-54b1-a436-37884e585f11")

  def selectorFor(uuid: UUID): Option[OccurrenceSelector] = {
    uuid.toString match {
      case "c7483fed-ff5c-54b1-a436-37884e585f11" =>
        Some(OccurrenceSelector("Animalia,Insecta", "ENVELOPE(-150,-50,40,10)", ""))
      case _ =>
        None
    }
  }
}

trait SubscriptionsStatic extends Subscriptions {
  def subscribersOf(selector: OccurrenceSelector): List[URL] = List(new URL("mailto:foo@bar"))

  def subscribe(subscriber: URL, selector: OccurrenceSelector) = new URL("mailto:foo@bar")

  def unsubscribe(subscriber: URL, selector: OccurrenceSelector) = new URL("mailto:foo@bar")

}

trait OccurrenceCollectionFetcherStatic extends OccurrenceCollectionFetcher {
  val anOccurrence = Occurrence("Cartoona | mickey", 12.1, 32.1, 123L, 124L, "recordId", 456L, "archiveId")
  val aSelector: OccurrenceSelector = OccurrenceSelector("Cartoona | mickey", "some wkt string", "some trait selector")
  val aMonitor = OccurrenceMonitor(aSelector, Some("some status"), Some(123))
  val anotherMonitor = OccurrenceMonitor(OccurrenceSelector("Cartoona | donald", "some wkt string", "some trait selector"), None, Some(123))

  def occurrencesFor(checklist: OccurrenceCollectionRequest): Iterator[Occurrence] = List(anOccurrence).iterator

  def monitoredOccurrencesFor(source: String, added: DateTimeSelector, occLimit: Option[Int]): Iterator[String] = List("some id", "another id").iterator

  def statusOf(selector: OccurrenceSelector): Option[String] = Some("ready")

  def request(selector: OccurrenceSelector): String = "requested"

  def monitors(): List[OccurrenceMonitor] = List(aMonitor, anotherMonitor)

  def monitorOf(selector: OccurrenceSelector): Option[OccurrenceMonitor] = Some(aMonitor)

  def monitorsFor(source: String, id: String): Iterator[OccurrenceSelector] = List(aSelector).iterator
}

class ChecklistService2Spec extends WordSpec with Matchers with ScalatestRouteTest with Service
  with SelectorRegistryStatic
  with SubscriptionsStatic
  with ChecklistFetcherStatic
  with OccurrenceCollectionFetcherStatic {

  "The service" should {
    "return a 'ping' response for GET requests to /ping" in {
      Get("/ping") ~> route ~> check {
        responseAs[String] shouldEqual "pong"
      }
    }

    "redirect to checklist viewer by uuid" in {
      Get("/view?uuid=c7483fed-ff5c-54b1-a436-37884e585f11") ~> route ~> check {
        status shouldEqual StatusCodes.TemporaryRedirect
      }
    }

    "return requested checklist" in {
      Get("/checklist?taxonSelector=Animalia,Insecta&wktString=ENVELOPE(-150,-50,40,10)") ~> route ~> check {
        responseAs[Checklist] shouldEqual Checklist(OccurrenceSelector("Animalia,Insecta", "ENVELOPE(-150,-50,40,10)", ""), "ready", List(ChecklistItem("donald", 1)))
      }
    }

    "return requested checklist uuid" in {
      Get("/checklist?uuid=c7483fed-ff5c-54b1-a436-37884e585f11") ~> route ~> check {
        responseAs[Checklist] shouldEqual Checklist(OccurrenceSelector("Animalia,Insecta", "ENVELOPE(-150,-50,40,10)", ""), "ready", List(ChecklistItem("donald", 1)))
      }
    }

    "return requested occurrenceCollection" in {
      Get("/occurrences?taxonSelector=Animalia,Insecta&wktString=ENVELOPE(-150,-50,40,10)") ~> route ~> check {
        responseAs[OccurrenceCollection] shouldEqual OccurrenceCollection(OccurrenceSelector("Animalia,Insecta", "ENVELOPE(-150,-50,40,10)", ""), Some("ready"), List(anOccurrence))
      }
    }

    "return requested occurrenceCollection uuid" in {
      Get("/occurrences?uuid=c7483fed-ff5c-54b1-a436-37884e585f11") ~> route ~> check {
        responseAs[OccurrenceCollection] shouldEqual OccurrenceCollection(OccurrenceSelector("Animalia,Insecta", "ENVELOPE(-150,-50,40,10)", ""), Some("ready"), List(anOccurrence))
      }
    }

    "return requested occurrenceCollection csv" in {
      Get("/occurrences.csv?taxonSelector=Animalia,Insecta&wktString=ENVELOPE(-150,-50,40,10)") ~> route ~> check {
        responseAs[String] should be(
          """taxon name,taxon path,lat,lng,eventStartDate,occurrenceId,firstAddedDate,source
            |"mickey","Cartoona | mickey",12.1,32.1,1970-01-01T00:00:00.123Z,"recordId",1970-01-01T00:00:00.456Z,"archiveId"
            |""".stripMargin)
      }
    }

    "return requested occurrenceCollection csv by uuid" in {
      Get("/occurrences.csv?uuid=c7483fed-ff5c-54b1-a436-37884e585f11") ~> route ~> check {
        responseAs[String] should be(
          """taxon name,taxon path,lat,lng,eventStartDate,occurrenceId,firstAddedDate,source
            |"mickey","Cartoona | mickey",12.1,32.1,1970-01-01T00:00:00.123Z,"recordId",1970-01-01T00:00:00.456Z,"archiveId"
            |""".stripMargin)
      }
    }


    "return requested monitored occurrences csv" in {
      Get("/monitoredOccurrences.csv?source=someSource") ~> route ~> check {
        responseAs[String] should be(
          """occurrenceId
            |"some id"
            |"another id"
            |""".stripMargin)
      }
    }

    "subscribe to monitor" in {
      Get("/subscribe?subscriber=mailto%3Afoo%40bar&taxonSelector=Animalia,Insecta&wktString=ENVELOPE(-150,-50,40,10)") ~> route ~> check {
        responseAs[String] should be("subscribed [mailto:foo@bar]")
      }
    }

    "subscribe to monitor uuid" in {
      Get("/subscribe?subscriber=mailto%3Afoo%40bar&uuid=c7483fed-ff5c-54b1-a436-37884e585f11") ~> route ~> check {
        responseAs[String] should be("subscribed [mailto:foo@bar]")
      }
    }

    "unsubscribe to selector" in {
      Get("/unsubscribe?subscriber=mailto%3Afoo%40bar&taxonSelector=Animalia,Insecta&wktString=ENVELOPE(-150,-50,40,10)") ~> route ~> check {
        responseAs[String] should be("unsubscribed [mailto:foo@bar]")
      }
    }

    "unsubscribe to selector uuid" in {
      Get("/unsubscribe?subscriber=mailto%3Afoo%40bar&uuid=c7483fed-ff5c-54b1-a436-37884e585f11") ~> route ~> check {
        responseAs[String] should be("unsubscribed [mailto:foo@bar]")
      }
    }

    "refresh monitor" in {
      Get("/update?taxonSelector=Animalia,Insecta&wktString=ENVELOPE(-150,-50,40,10)") ~> route ~> check {
        responseAs[OccurrenceCollection] shouldEqual OccurrenceCollection(OccurrenceSelector("Animalia,Insecta", "ENVELOPE(-150,-50,40,10)", ""), Some("requested"), List())
      }
    }

    "refresh monitor uuid" in {
      Get("/update?uuid=c7483fed-ff5c-54b1-a436-37884e585f11") ~> route ~> check {
        responseAs[OccurrenceCollection] shouldEqual OccurrenceCollection(OccurrenceSelector("Animalia,Insecta", "ENVELOPE(-150,-50,40,10)", ""), Some("requested"), List())
      }
    }

    "send notification to subscribers" in {
      Get("/notify?addedAfter=2016-01-10&taxonSelector=Animalia,Insecta&wktString=ENVELOPE(-150,-50,40,10)") ~> route ~> check {
        responseAs[String] should be("change detected: sent notifications")
      }
    }

    "send notification to subscribers uuid" in {
      Get("/notify?addedAfter=2016-01-10&uuid=c7483fed-ff5c-54b1-a436-37884e585f11") ~> route ~> check {
        responseAs[String] should be("change detected: sent notifications")
      }
    }

    "return requested monitors" in {
      Get("/monitors") ~> route ~> check {
        responseAs[List[OccurrenceMonitor]] should contain(OccurrenceMonitor(OccurrenceSelector("Cartoona | mickey", "some wkt string", "some trait selector"), Some("some status"), Some(123)))
      }
    }

    "return single monitor" in {
      Get("/monitors?taxonSelector=Animalia,Insecta&wktString=ENVELOPE(-150,-50,40,10)") ~> route ~> check {
        responseAs[OccurrenceMonitor] should be(OccurrenceMonitor(OccurrenceSelector("Cartoona | mickey", "some wkt string", "some trait selector"), Some("some status"), Some(123)))
      }
    }

    "return single monitor uuid" in {
      Get("/monitors?uuid=c7483fed-ff5c-54b1-a436-37884e585f11") ~> route ~> check {
        responseAs[OccurrenceMonitor] should be(OccurrenceMonitor(OccurrenceSelector("Cartoona | mickey", "some wkt string", "some trait selector"), Some("some status"), Some(123)))
      }
    }

    "return monitors for" in {
      Get("/monitors?source=someSource&id=someId") ~> route ~> check {
        responseAs[List[OccurrenceSelector]] should contain(OccurrenceSelector("Cartoona | mickey", "some wkt string", "some trait selector"))
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
