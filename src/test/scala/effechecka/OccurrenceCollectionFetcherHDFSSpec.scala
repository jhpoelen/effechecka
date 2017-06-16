package effechecka

import org.scalatest.{Matchers, WordSpec}

class OccurrenceCollectionFetcherHDFSSpec extends WordSpec with Matchers with OccurrenceCollectionFetcherHDFS {

  "Cassandra driver" should {
    "store and provide access to an occurrence collection" in {
      val selector: OccurrenceSelector = OccurrenceSelector("Animalia|Insecta", "ENVELOPE(-150,-50,40,10)", "")
      val request = OccurrenceRequest(selector, Some(2))
      val occurrenceCollection = occurrencesFor(request)
      val occ = occurrenceCollection.next
      occ.lat should be(12.1 +- 1e-2)
      occ.lng should be(11.1 +- 1e-2)
      occ.taxon should be("a|taxon")
      occ.start should be(0L)
      occ.end should be(1L)
      occ.added should be(2L)
      occ.id should be("some:id")
      occ.source should be("a source")

      //monitors() should contain(OccurrenceMonitor(OccurrenceSelector("Animalia|Insecta", "ENVELOPE(-150,-50,40,10)", "").withUUID, Some("requested"), Some(0)))
      //monitorOf(OccurrenceSelector("Insecta|Aves", "wktString", "")) should contain(OccurrenceMonitor(OccurrenceSelector("Insecta|Aves", "wktString", "").withUUID, Some("requested"), Some(0)))
      //monitorOf(OccurrenceSelector("Insecta", "wktString", "")) should contain(OccurrenceMonitor(OccurrenceSelector("Insecta", "wktString", "").withUUID, Some("requested"), Some(0)))
    }

    "occurrence selector with null status" in {
      val expectedWktString: String = "ENVELOPE(-150,-50,40,10)"
      val expectedTraitSelector: String = ""
      val expectedTaxonSelector: String = "Animalia|Insecta"
      val expectedSelector: OccurrenceSelector = OccurrenceSelector(expectedTaxonSelector, expectedWktString, expectedTraitSelector)
      monitors() should contain(OccurrenceMonitor(expectedSelector, None, Some(123)))
      monitorOf(OccurrenceSelector("Animalia|Insecta", expectedWktString, expectedTraitSelector)) should be(Some(OccurrenceMonitor(OccurrenceSelector("Animalia|Insecta", expectedWktString, expectedTraitSelector), None, Some(123))))
    }

    "return monitored occurrences" in {
      val expectedWktString: String = "ENVELOPE(-150,-50,40,10)"
      val expectedTraitSelector: String = ""
      val expectedTaxonSelector: String = "Animalia|Insecta"
      val occIter: Iterator[String] = monitoredOccurrencesFor("my source")
      occIter.next() should be("some:id")
      occIter.hasNext should be(false)

      val monitors: Iterator[OccurrenceSelector] = monitorsFor(source = "my source", id = "some id")
      monitors.next() should be(OccurrenceSelector(expectedTaxonSelector, expectedWktString, expectedTraitSelector))
      monitors.hasNext should be(false)
    }

  }

}
