package effechecka

import org.scalatest.{Matchers, WordSpec}

class OccurrenceCollectionFetcherSpec extends WordSpec with Matchers with OccurrenceCollectionFetcherCassandra with Configure {

  "Cassandra driver" should {
    "store and provide access to an occurrence collection" in {
      val request = OccurrenceCollectionRequest(OccurrenceSelector("Insecta|Mammalia", "ENVELOPE(-150,-50,40,10)", "bodyMass greaterThan 2.7 kg"), 2)
      session.execute("TRUNCATE effechecka.occurrence_collection")
      insertRequest(request)
      session.execute("INSERT INTO effechecka.occurrence_collection (taxonselector, wktstring, traitSelector, taxon, lat, lng, start, end, id, added, source) " +
        "VALUES ('Insecta|Mammalia', 'ENVELOPE(-150,-50,40,10)', 'bodyMass greaterThan 2.7 kg', 'Aves|Donald duckus', 12.1, 11.1, 1234, 1235, 'http://record.url', '2012-02-02T04:23:01.000Z', 'http://archive.url')")
      val occurrenceCollection = occurrencesFor(request)
      val occ = occurrenceCollection.head
      occ.lat should be(12.1 +- 1e-2)
      occ.lng should be(11.1 +- 1e-2)
      occ.taxon should be("Aves|Donald duckus")
      occ.start should be(1234L)
      occ.end should be(1235L)
      occ.id should be("http://record.url")
      occ.source should be("http://archive.url")
      occ.added should be(1328156581000L)

      monitors() should contain(OccurrenceMonitor(OccurrenceSelector("Insecta|Mammalia", "ENVELOPE(-150,-50,40,10)", "bodyMass greaterThan 2.7 kg"), "requested", 0))
    }
  }

}
