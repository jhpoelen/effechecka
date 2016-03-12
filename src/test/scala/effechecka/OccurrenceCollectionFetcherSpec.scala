package effechecka

import org.scalatest.{Matchers, WordSpec}

class OccurrenceCollectionFetcherSpec extends WordSpec with Matchers with OccurrenceCollectionFetcherCassandra with Configure {

  "Cassandra driver" should {
    "store and provide access to an occurrence collection" in {
      val request = OccurrenceCollectionRequest("Insecta|Mammalia", "ENVELOPE(-150,-50,40,10)", "bodyMass greaterThan 2.7 kg", 2)
      session.execute("TRUNCATE effechecka.occurrence_collection")
      insertRequest(request)
      session.execute("INSERT INTO effechecka.occurrence_collection (taxonselector, wktstring, traitSelector, taxon, lat, lng, event_date, record_url, first_added_date, archive_url) " +
        "VALUES ('Insecta|Mammalia', 'ENVELOPE(-150,-50,40,10)', 'bodyMass greaterThan 2.7 kg', 'Aves|Donald duckus', 12.1, 11.1, '2010-01-01T10:10:00.000Z', 'http://record.url', '2012-02-02T04:23:01.000Z', 'http://archive.url')")
      val occurrenceCollection = occurrencesFor(request)
      val occ = occurrenceCollection.head
      occ.lat should be(12.1 +- 1e-2)
      occ.lng should be(11.1 +- 1e-2)
      occ.taxon should be("Aves|Donald duckus")
      occ.eventDate should be(1262340600000L)
      occ.recordUrl should be("http://record.url")
      occ.archiveUrl should be("http://archive.url")
      occ.firstAddedDate should be(1328156581000L)
    }
  }

}
