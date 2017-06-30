package effechecka

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import org.effechecka.selector.OccurrenceSelector
import org.scalatest.{Matchers, WordSpecLike}

class OccurrenceCollectionFetcherHDFSSpec extends TestKit(ActorSystem("IntegrationTest"))
  with WordSpecLike with Matchers with OccurrenceCollectionFetcherHDFS with HDFSTestUtil {

  implicit val materializer = ActorMaterializer()(system)
  implicit val ec = system.dispatcher

  val expectedWktString: String = "ENVELOPE(-150,-50,40,10)"
  val expectedTraitSelector: String = ""
  val expectedSelector = OccurrenceSelector("Animalia|Insecta", expectedWktString, expectedTraitSelector, Some("55e4b0a0-bcd9-566f-99bc-357439011d85"))
  val expectedMonitor = OccurrenceMonitor(expectedSelector, Some("ready"), Some(3997))

  "HDFS" should {
    "access an occurrence collection" in {
      val selector: OccurrenceSelector = OccurrenceSelector("Animalia|Insecta", "ENVELOPE(-150,-50,40,10)", "")
      val request = OccurrenceRequest(selector, Some(2))
      val occurrenceCollection = occurrencesFor(request)
      occurrenceCollection.hasNext shouldBe true
      val occ = occurrenceCollection.next
      occ.lat should be(37.72 +- 1e-2)
      occ.lng should be(-122.42 +- 1e-2)
      occ.taxon should be("Plantae|Magnoliophyta|Magnoliopsida|Apiales|Araliaceae|Hedera|Hedera helix")
      occ.start should be(1415062695000L)
      occ.end should be(1415062695000L)
      occ.added should be(1433462400000L)
      occ.id should be("http://www.inaturalist.org/observations/1053719")
      occ.source should be("inaturalist")

      occurrenceCollection.take(2).length shouldBe 1
    }

    "occurrence selector with null status" in {
      monitors() should contain(expectedMonitor)
      monitorOf(expectedSelector) should be(Some(expectedMonitor))
    }

    "return monitored occurrences by source and/or occurrence id" in {
      val occIter: Iterator[String] = monitoredOccurrencesFor("inaturalist")
      occIter.next() should be("http://www.inaturalist.org/observations/1053719")
      occIter.hasNext should be(false)

      val monitors: Iterator[OccurrenceSelector] = monitorsFor(source = "inaturalist", id = "http://www.inaturalist.org/observations/1053719")
      monitors.next() should be(expectedSelector)
      monitors.hasNext should be(false)
    }

    "return status of monitor" in {
      statusOf(expectedSelector) should be(Some("ready"))
      statusOf(OccurrenceSelector("no|taxon", expectedWktString, "")) should be(None)
    }

    "already requested" in {
      request(expectedSelector) should be("ready")
    }

  }


}
