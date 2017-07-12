package effechecka

import akka.NotUsed
import akka.http.scaladsl.model._
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.scaladsl.Source
import akka.util.ByteString
import org.effechecka.selector.DateTimeSelector
import org.scalatest.{Matchers, WordSpec}

trait ChecklistFetcherStatic extends ChecklistFetcher {
  def itemsFor(checklist: ChecklistRequest): Iterator[ChecklistItem] = Iterator(ChecklistItem("donald", 1))

  def statusOf(checklist: ChecklistRequest): Option[String] = Some("ready")

  def tsvFor(checklist: ChecklistRequest): Source[ByteString, NotUsed]
  = Source.fromIterator(() => Iterator(ByteString("taxonName\ttaxonPath\trecordCount\ndonald\tdonald\t1")))
}

trait ChecklistFetcherExploding extends ChecklistFetcher {
  def itemsFor(checklist: ChecklistRequest): Iterator[ChecklistItem] = throw new RuntimeException("kaboom!")

  def statusOf(checklist: ChecklistRequest): Option[String] = Some("ready")

  def tsvFor(checklist: ChecklistRequest): Source[ByteString, NotUsed] = Source.fromIterator(() => Iterator())

}

trait ChecklistFetcherEmpty extends ChecklistFetcher {
  def itemsFor(checklist: ChecklistRequest): Iterator[ChecklistItem] = throw new RuntimeException("kaboom!")

  def statusOf(checklist: ChecklistRequest): Option[String] = None

  def tsvFor(checklist: ChecklistRequest): Source[ByteString, NotUsed] = Source.fromIterator(() => Iterator())

}

trait OccurrenceCollectionFetcherEmpty extends OccurrenceCollectionFetcher {
  def occurrencesTsvFor(checklist: OccurrenceRequest): Source[ByteString, NotUsed] = throw new IllegalArgumentException()

  def occurrencesFor(checklist: OccurrenceRequest): Iterator[Occurrence] = throw new IllegalArgumentException()

  def monitoredOccurrencesFor(source: String, added: DateTimeSelector, occLimit: Option[Int]): Source[ByteString, NotUsed]
   = throw new IllegalArgumentException()

  def statusOf(selector: Selector): Option[String] = None

  def monitors(): List[OccurrenceMonitor] = throw new IllegalArgumentException()

  def monitorOf(selector: Selector): Option[OccurrenceMonitor] = throw new IllegalArgumentException()

}

trait OccurrenceCollectionFetcherStatic extends OccurrenceCollectionFetcher {
  val anOccurrence = Occurrence("Cartoona | mickey", 12.1, 32.1, 123L, 124L, "recordId", 456L, "archiveId")
  val aSelector = SelectorParams("Cartoona | mickey", "some wkt string", "some trait selector").withUUID()
  val aMonitor = OccurrenceMonitor(aSelector, Some("some status"), Some(123))
  val anotherMonitor = OccurrenceMonitor(SelectorParams("Cartoona | donald", "some wkt string", "some trait selector").withUUID(), None, Some(123))

  def occurrencesTsvFor(checklist: OccurrenceRequest): Source[ByteString, NotUsed]
  = Source.fromIterator(() => Iterator(ByteString.fromString("taxonName\ttaxonPath\tlat\tlng\teventStartDate\toccurrenceId\tfirstAddedDate\tsource\toccurrenceUrl"),
    ByteString.fromString(CsvUtils.toOccurrenceRow(anOccurrence))))

  def occurrencesFor(checklist: OccurrenceRequest): Iterator[Occurrence] = List(anOccurrence).iterator

  def monitoredOccurrencesFor(source: String, added: DateTimeSelector, occLimit: Option[Int]): Source[ByteString, NotUsed]
  = Source.fromIterator(() =>
    Iterator(ByteString.fromString("occurrenceId\tmonitorUUID"), ByteString.fromString("\nsome id\t"),
      ByteString.fromString("\nanother id\tsomeUUID")))

  def statusOf(selector: Selector): Option[String] = Some("ready")

  def monitors(): List[OccurrenceMonitor] = List(aMonitor, anotherMonitor)

  def monitorOf(selector: Selector): Option[OccurrenceMonitor] = Some(aMonitor)

}

trait JobSubmitterStatic extends JobSubmitter {

  override def submit(selector: SelectorParams, jobMainClass: String): SparkDispatchResponse = {
    SparkDispatchResponse(action = "SomeTestAction", message = Some("some message"))
  }
}

class WebApiExplodingSpec extends WordSpec with Matchers with ScalatestRouteTest
  with Service
  with JobSubmitterStatic
  with ChecklistFetcherExploding
  with OccurrenceCollectionFetcherStatic {

  "The service" should {
    "close iterator on exploding" in {
      Get("/checklist?taxonSelector=Animalia,Insecta&wktString=ENVELOPE(-150,-50,40,10)") ~> route ~> check {
        status shouldEqual StatusCodes.InternalServerError
      }
    }
  }
}

class WebApiSubmitSpec extends WordSpec with Matchers with ScalatestRouteTest
  with Service
  with JobSubmitterStatic
  with ChecklistFetcherEmpty
  with OccurrenceCollectionFetcherEmpty {

  "checklist service" should {
    "provide a submission message" in {
      Get("/checklist?taxonSelector=Animalia,Insecta&wktString=ENVELOPE(-150,-50,40,10)") ~> route ~> check {
        val selector = SelectorUUID("55e4b0a0-bcd9-566f-99bc-357439011d85", Some("Animalia|Insecta"), Some("ENVELOPE(-150,-50,40,10)"), Some(""))
        responseAs[Checklist] shouldBe Checklist(selector, "some message", List())
      }
    }
    "provide an appropriate status code" in {
      Get("/checklist.tsv?taxonSelector=Animalia,Insecta&wktString=ENVELOPE(-150,-50,40,10)") ~> route ~> check {
        status shouldBe StatusCodes.BadRequest
      }
    }
  }

  "occurrence service" should {
    "provide a submission message" in {
      Get("/occurrences?taxonSelector=Animalia,Insecta&wktString=ENVELOPE(-150,-50,40,10)") ~> route ~> check {
        val selector = SelectorUUID("55e4b0a0-bcd9-566f-99bc-357439011d85", Some("Animalia|Insecta"), Some("ENVELOPE(-150,-50,40,10)"), Some(""))
        responseAs[OccurrenceCollection] shouldBe OccurrenceCollection(selector, Some("some message"), List())
      }
    }
    "provide an appropriate status code" in {
      Get("/occurrences.tsv?taxonSelector=Animalia,Insecta&wktString=ENVELOPE(-150,-50,40,10)") ~> route ~> check {
        status shouldBe StatusCodes.BadRequest
      }
    }
  }
}

class WebApiSpec extends WordSpec with Matchers
  with ScalatestRouteTest
  with Service
  with JobSubmitterStatic
  with ChecklistFetcherStatic
  with OccurrenceCollectionFetcherStatic {

  "The service" should {
    "return a 'ping' response for GET requests to /ping" in {
      Get("/ping") ~> route ~> check {
        responseAs[String] shouldEqual "pong"
      }
    }

    val selectorAnimaliaInsecta = SelectorUUID("55e4b0a0-bcd9-566f-99bc-357439011d85", Some("Animalia|Insecta"), Some("ENVELOPE(-150,-50,40,10)"), Some(""))

    "return requested checklist" in {
      Get("/checklist?taxonSelector=Animalia,Insecta&wktString=ENVELOPE(-150,-50,40,10)") ~> route ~> check {
        responseAs[Checklist] shouldEqual Checklist(selectorAnimaliaInsecta, "ready", List(ChecklistItem("donald", 1)))
      }
    }

    "return requested checklist.tsv" in {
      Get("/checklist.tsv?taxonSelector=Animalia,Insecta&wktString=ENVELOPE(-150,-50,40,10)") ~> route ~> check {
        responseAs[String] shouldEqual "taxonName\ttaxonPath\trecordCount\ndonald\tdonald\t1"
      }
    }

    "return requested checklist uuid" in {
      Get("/checklist?uuid=55e4b0a0-bcd9-566f-99bc-357439011d85") ~> route ~> check {
        responseAs[Checklist] shouldEqual Checklist(SelectorUUID("55e4b0a0-bcd9-566f-99bc-357439011d85"), "ready", List(ChecklistItem("donald", 1)))
      }
    }

    "return requested occurrenceCollection" in {
      Get("/occurrences?taxonSelector=Animalia,Insecta&wktString=ENVELOPE(-150,-50,40,10)") ~> route ~> check {
        responseAs[OccurrenceCollection] shouldEqual OccurrenceCollection(selectorAnimaliaInsecta, Some("ready"), List(anOccurrence))
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
        responseAs[OccurrenceCollection] shouldEqual OccurrenceCollection(SelectorUUID("5ffd7bae-5fe0-5692-b914-bf90e921fa1b", Some("Animalia|Insecta"), Some("POLYGON ((-150 10, -150 40, -50 40, -50 10, -150 10))"), Some("")), Some("ready"), List(anOccurrence))
      }
    }

    "return requested occurrenceCollection uuid" in {
      Get("/occurrences?uuid=55e4b0a0-bcd9-566f-99bc-357439011d85") ~> route ~> check {
        responseAs[OccurrenceCollection] shouldEqual OccurrenceCollection(SelectorUUID("55e4b0a0-bcd9-566f-99bc-357439011d85"), Some("ready"), List(anOccurrence))
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
          "occurrenceId\tmonitorUUID\n" +
            "some id\t\n" +
            "another id\tsomeUUID")
      }
    }
    val expectedCartoon = SelectorUUID("cd459078-feb1-5b84-b21e-fead00792c7d", Some("Cartoona | mickey"), Some("some wkt string"), Some("some trait selector"))

    "return requested monitors" in {
      Get("/monitors") ~> route ~> check {
        responseAs[List[OccurrenceMonitor]] should contain(OccurrenceMonitor(expectedCartoon, Some("some status"), Some(123)))
      }
    }

    "return single monitor" in {
      Get("/monitors?taxonSelector=Animalia,Insecta&wktString=ENVELOPE(-150,-50,40,10)") ~> route ~> check {
        responseAs[OccurrenceMonitor] should be(OccurrenceMonitor(expectedCartoon, Some("some status"), Some(123)))
      }
    }

    "return single monitor uuid" in {
      Get("/monitors?uuid=55e4b0a0-bcd9-566f-99bc-357439011d85") ~> route ~> check {
        responseAs[OccurrenceMonitor] should be(OccurrenceMonitor(expectedCartoon, Some("some status"), Some(123)))
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
