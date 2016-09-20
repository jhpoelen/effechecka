package effechecka

import java.net.URL
import java.util.UUID


import akka.http.scaladsl.model.TransferEncodings.{gzip, deflate}
import akka.http.scaladsl.model.headers.{`Access-Control-Allow-Origin`, Location, `Accept-Encoding`}
import akka.http.scaladsl.server.ValidationRejection
import org.scalatest.{Matchers, WordSpec}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.http.scaladsl.model._

trait ChecklistFetcherStatic extends ChecklistFetcher {
  def itemsFor(checklist: ChecklistRequest): List[ChecklistItem] = List(ChecklistItem("donald", 1))

  def statusOf(checklist: ChecklistRequest): Option[String] = Some("ready")

  def request(checklist: ChecklistRequest): String = "requested"
}

trait SelectorRegistryStatic extends SelectorRegistry {
  val selector = OccurrenceSelector("Animalia|Insecta", "ENVELOPE(-150,-50,40,10)", "")


  def registerSelector(selector: OccurrenceSelector, ttlSeconds: Option[Int] = None): UUID = {
    UUID.fromString("55e4b0a0-bcd9-566f-99bc-357439011d85")
  }

  def unregisterSelectors(filter: (OccurrenceSelector) => Boolean): List[OccurrenceSelector] = {
    List(selector)
  }

  def selectorFor(uuid: UUID): Option[OccurrenceSelector] = {
    uuid.toString match {
      case "55e4b0a0-bcd9-566f-99bc-357439011d85" =>
        Some(selector)
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

  def occurrencesFor(checklist: OccurrenceRequest): Iterator[Occurrence] = List(anOccurrence).iterator

  def monitoredOccurrencesFor(source: String, added: DateTimeSelector, occLimit: Option[Int]): Iterator[String] = List("some id", "another id").iterator

  def statusOf(selector: OccurrenceSelector): Option[String] = Some("ready")

  def request(selector: OccurrenceSelector): String = "requested"

  def requestAll(): String = "all requested"

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
      Get("/view?uuid=55e4b0a0-bcd9-566f-99bc-357439011d85") ~> route ~> check {
        val uri = "http://gimmefreshdata.github.io/?taxonSelector=Animalia%7CInsecta&wktString=ENVELOPE%28-150%2C-50%2C40%2C10%29&traitSelector="
        response shouldEqual HttpResponse(
          status = StatusCodes.TemporaryRedirect,
          entity = HttpEntity(
            ContentTypes.`text/html(UTF-8)`,
            s"""The request should be repeated with <a href="$uri">this URI</a>, but future requests can still use the original URI."""),
          headers = `Access-Control-Allow-Origin`.* :: Location(uri) :: Nil)
      }
    }

    "redirect to checklist viewer by uuid with added" in {
      Get("/view?uuid=55e4b0a0-bcd9-566f-99bc-357439011d85&addedAfter=2017-01-01") ~> route ~> check {
        val uri = "http://gimmefreshdata.github.io/?taxonSelector=Animalia%7CInsecta&wktString=ENVELOPE%28-150%2C-50%2C40%2C10%29&traitSelector=&addedAfter=2017-01-01"
        response shouldEqual HttpResponse(
          status = StatusCodes.TemporaryRedirect,
          entity = HttpEntity(
            ContentTypes.`text/html(UTF-8)`,
            s"""The request should be repeated with <a href="$uri">this URI</a>, but future requests can still use the original URI."""),
          headers = `Access-Control-Allow-Origin`.* :: Location(uri) :: Nil)
      }
    }

    "return requested checklist" in {
      Get("/checklist?taxonSelector=Animalia,Insecta&wktString=ENVELOPE(-150,-50,40,10)") ~> route ~> check {
        responseAs[Checklist] shouldEqual Checklist(OccurrenceSelector("Animalia|Insecta", "ENVELOPE(-150,-50,40,10)", ""), "ready", List(ChecklistItem("donald", 1)))
      }
    }

    "return requested checklist uuid" in {
      Get("/checklist?uuid=55e4b0a0-bcd9-566f-99bc-357439011d85") ~> route ~> check {
        responseAs[Checklist] shouldEqual Checklist(OccurrenceSelector("Animalia|Insecta", "ENVELOPE(-150,-50,40,10)", ""), "ready", List(ChecklistItem("donald", 1)))
      }
    }

    "return requested occurrenceCollection" in {
      Get("/occurrences?taxonSelector=Animalia,Insecta&wktString=ENVELOPE(-150,-50,40,10)") ~> route ~> check {
        responseAs[OccurrenceCollection] shouldEqual OccurrenceCollection(OccurrenceSelector("Animalia|Insecta", "ENVELOPE(-150,-50,40,10)", ""), Some("ready"), List(anOccurrence))
      }
    }

    "occurrenceCollection request invalid taxon" in {
      Get("/occurrences?taxonSelector=%2Fetc%2Fpassword&wktString=ENVELOPE(-150,-50,40,10)") ~> route ~> check {
        assertBadRequest
      }
    }

    "occurrenceCollection request invalid wktString" in {
      Get("/occurrences?taxonSelector=Animalia,Insecta&wktString=DUCK(-150,-50,40,10)") ~> route ~> check {
        assertBadRequest
      }
    }

    "return requested occurrenceCollection error" in {
      Get("/occurrences?limit=20&taxonSelector=Animalia%2CInsecta&wktString=POLYGON%20((-150%2010%2C%20-150%2040%2C%20-50%2040%2C%20-50%2010%2C%20-150%2010))") ~> route ~> check {
        responseAs[OccurrenceCollection] shouldEqual OccurrenceCollection(OccurrenceSelector("Animalia|Insecta", "POLYGON ((-150 10, -150 40, -50 40, -50 10, -150 10))", ""), Some("ready"), List(anOccurrence))
      }
    }

    "return requested occurrenceCollection uuid" in {
      Get("/occurrences?uuid=55e4b0a0-bcd9-566f-99bc-357439011d85") ~> route ~> check {
        responseAs[OccurrenceCollection] shouldEqual OccurrenceCollection(OccurrenceSelector("Animalia|Insecta", "ENVELOPE(-150,-50,40,10)", ""), Some("ready"), List(anOccurrence))
      }
    }

    "return requested occurrenceCollection tsv" in {
      Get("/occurrences.tsv?taxonSelector=Animalia,Insecta&wktString=ENVELOPE(-150,-50,40,10)") ~> route ~> check {
        responseAs[String] should be(
          "taxonName\ttaxonPath\tlat\tlng\teventStartDate\toccurrenceId\tfirstAddedDate\tsource\toccurrenceUrl\n" +
            "mickey\tCartoona | mickey\t12.1\t32.1\t1970-01-01T00:00:00.123Z\trecordId\t1970-01-01T00:00:00.456Z\tarchiveId\t")
      }
    }

    "return requested occurrenceCollection tsv by uuid" in {
      Get("/occurrences.tsv?uuid=55e4b0a0-bcd9-566f-99bc-357439011d85") ~> route ~> check {
        responseAs[String] should be(
          "taxonName\ttaxonPath\tlat\tlng\teventStartDate\toccurrenceId\tfirstAddedDate\tsource\toccurrenceUrl\n" +
            "mickey\tCartoona | mickey\t12.1\t32.1\t1970-01-01T00:00:00.123Z\trecordId\t1970-01-01T00:00:00.456Z\tarchiveId\t")
      }
    }


    "return requested monitored occurrences tsv" in {
      Get("/monitoredOccurrences.tsv?source=someSource") ~> route ~> check {
        responseAs[String] should be(
          """occurrenceId
            |some id
            |another id""".stripMargin)
      }
    }

    "subscribe to monitor" in {
      Get("/subscribe?subscriber=mailto%3Afoo%40bar&taxonSelector=Animalia,Insecta&wktString=ENVELOPE(-150,-50,40,10)") ~> route ~> check {
        responseAs[String] should be("subscribed [mailto:foo@bar]")
      }
    }

    "subscribe to monitor uuid" in {
      Get("/subscribe?subscriber=mailto%3Afoo%40bar&uuid=55e4b0a0-bcd9-566f-99bc-357439011d85") ~> route ~> check {
        responseAs[String] should be("subscribed [mailto:foo@bar]")
      }
    }

    "unsubscribe to selector" in {
      Get("/unsubscribe?subscriber=mailto%3Afoo%40bar&taxonSelector=Animalia,Insecta&wktString=ENVELOPE(-150,-50,40,10)") ~> route ~> check {
        responseAs[String] should be("unsubscribed [mailto:foo@bar]")
      }
    }

    "unsubscribe to selector uuid" in {
      Get("/unsubscribe?subscriber=mailto%3Afoo%40bar&uuid=55e4b0a0-bcd9-566f-99bc-357439011d85") ~> route ~> check {
        responseAs[String] should be("unsubscribed [mailto:foo@bar]")
      }
    }

    "refresh all monitors" in {
      Get("/updateAll") ~> route ~> check {
        responseAs[String] shouldEqual "all requested"
      }
    }

    "refresh monitor uuid" in {
      Get("/update?uuid=55e4b0a0-bcd9-566f-99bc-357439011d85") ~> route ~> check {
        responseAs[OccurrenceCollection] shouldEqual OccurrenceCollection(OccurrenceSelector("Animalia|Insecta", "ENVELOPE(-150,-50,40,10)", ""), Some("requested"), List())
      }
    }

    "send notification to subscribers" in {
      Get("/notify?addedAfter=2016-01-10&taxonSelector=Animalia,Insecta&wktString=ENVELOPE(-150,-50,40,10)") ~> route ~> check {
        responseAs[String] should be("sent [1] notification related occurrences added [DateTimeSelector(None,Some(2016-01-10))] to monitors [OccurrenceSelector(Animalia|Insecta,ENVELOPE(-150,-50,40,10),,None)]")
      }
    }

    "send notification to subscribers uuid" in {
      Get("/notify?addedAfter=2016-01-10&uuid=55e4b0a0-bcd9-566f-99bc-357439011d85") ~> route ~> check {
        responseAs[String] should be("sent [1] notification related occurrences added [DateTimeSelector(None,Some(2016-01-10))] to monitors [OccurrenceSelector(Animalia|Insecta,ENVELOPE(-150,-50,40,10),,None)]")
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
      Get("/monitors?uuid=55e4b0a0-bcd9-566f-99bc-357439011d85") ~> route ~> check {
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
        assertBadRequest
      }
    }

  }

  def assertBadRequest: Unit = {
    status shouldEqual StatusCodes.BadRequest
  }
}
