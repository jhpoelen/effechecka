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

    "store and provide access to an occurrence collection within added constraints" in {
      val addedDateString: String = "'1970-01-01T00:00:00.000Z'"
      assertCountForAddedRange(Some("1971-01-01"), Some("1969-01-01"), addedDateString, 1)
      assertCountForAddedRange(Some("1971-01-01"), None, addedDateString, 1)
      assertCountForAddedRange(None, Some("1969-01-01"), addedDateString, 1)
      assertCountForAddedRange(None, Some("1972-01-01"), addedDateString, 0)
      assertCountForAddedRange(Some("1999-01-01"), Some("1972-01-01"), addedDateString, 0)
    }
  }

  def assertCountForAddedRange(addedBefore: Option[String], addedAfter: Option[String], addedDateString: String, expectedOccurrenceCount: Int): Unit = {
    val request: OccurrenceCollectionRequest = occurrenceQuery(addedBefore, addedAfter)
    session.execute("TRUNCATE effechecka.occurrence_collection")
    insertRequest(request)

    session.execute("INSERT INTO effechecka.occurrence_collection (taxonselector, wktstring, traitSelector, taxon, lat, lng, start, end, id, added, source) " +
      "VALUES ('Insecta|Mammalia', 'ENVELOPE(-150,-50,40,10)', 'bodyMass greaterThan 2.7 kg', 'Aves|Donald duckus', 12.1, 11.1, 1234, 1235, 'http://record.url'," + addedDateString + ", 'http://archive.url')")
    val occurrenceCollection = occurrencesFor(request)
    occurrenceCollection.length should be(expectedOccurrenceCount)
  }

  def occurrenceQuery(addedBefore: Option[String], addedAfter: Option[String]): OccurrenceCollectionRequest = {
    OccurrenceCollectionRequest(OccurrenceSelector("Insecta|Mammalia", "ENVELOPE(-150,-50,40,10)", "bodyMass greaterThan 2.7 kg"), 2, addedBefore, addedAfter)
  }
}
