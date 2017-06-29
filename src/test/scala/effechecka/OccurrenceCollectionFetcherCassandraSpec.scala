package effechecka

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import com.datastax.driver.core.ResultSet
import org.effechecka.selector.{DateTimeSelector, OccurrenceSelector}
import org.scalatest.{Matchers, WordSpecLike}

class OccurrenceCollectionFetcherCassandraSpec  extends TestKit(ActorSystem("SparkIntegrationTest"))
  with WordSpecLike with Matchers with OccurrenceCollectionFetcherCassandra with Configure {

  implicit val materializer = ActorMaterializer()(system)
  implicit val ec = system.dispatcher

  "Cassandra driver" should {
    "store and provide access to an occurrence collection" in {
      val selector: OccurrenceSelector = OccurrenceSelector("Insecta|Mammalia", "ENVELOPE(-150,-50,40,10)", "bodyMass greaterThan 2.7 kg")
      val request = OccurrenceRequest(selector, Some(2))
      truncate
      insertRequest(OccurrenceSelector("Insecta", "wktString", ""))
      insertRequest(selector)
      insertRequest(OccurrenceSelector("Insecta|Aves", "wktString", ""))
      session.execute("INSERT INTO effechecka.occurrence_collection (taxonselector, wktstring, traitSelector, taxon, lat, lng, start, end, id, added, source) " +
        "VALUES ('Insecta|Mammalia', 'ENVELOPE(-150,-50,40,10)', 'bodyMass greaterThan 2.7 kg', 'Aves|Donald duckus', 12.1, 11.1, 1234, 1235, 'http://record.url', '2012-02-02T04:23:01.000Z', 'http://archive.url')")
      val occurrenceCollection = occurrencesFor(request)
      val occ = occurrenceCollection.next
      occ.lat should be(12.1 +- 1e-2)
      occ.lng should be(11.1 +- 1e-2)
      occ.taxon should be("Aves|Donald duckus")
      occ.start should be(1234L)
      occ.end should be(1235L)
      occ.id should be("http://record.url")
      occ.source should be("http://archive.url")
      occ.added should be(1328156581000L)

      monitors() should contain(OccurrenceMonitor(OccurrenceSelector("Insecta|Mammalia", "ENVELOPE(-150,-50,40,10)", "bodyMass greaterThan 2.7 kg").withUUID, Some("requested"), Some(0)))
      monitorOf(OccurrenceSelector("Insecta|Aves", "wktString", "")) should contain(OccurrenceMonitor(OccurrenceSelector("Insecta|Aves", "wktString", "").withUUID, Some("requested"), Some(0)))
      monitorOf(OccurrenceSelector("Insecta", "wktString", "")) should contain(OccurrenceMonitor(OccurrenceSelector("Insecta", "wktString", "").withUUID, Some("requested"), Some(0)))
    }

    "occurrence selector with null status" in {
      truncate
      session.execute("INSERT INTO effechecka.occurrence_collection_registry (taxonselector, wktstring, traitSelector, recordcount) " +
              "VALUES ('Insecta|Mammalia', 'ENVELOPE(-150,-50,40,10)', 'bodyMass greaterThan 2.7 kg', 123)")
      session.execute("INSERT INTO effechecka.occurrence_collection_registry (taxonselector, wktstring, traitSelector) " +
              "VALUES ('Aves|Mammalia', 'ENVELOPE(-150,-50,40,10)', 'bodyMass greaterThan 2.7 kg')")
      val expectedWktString: String = "ENVELOPE(-150,-50,40,10)"
      val expectedTraitSelector: String = "bodyMass greaterThan 2.7 kg"
      val expectedTaxonSelector: String = "Insecta|Mammalia"
      val expectedSelector: OccurrenceSelector = OccurrenceSelector(expectedTaxonSelector, expectedWktString, expectedTraitSelector)
      monitors() should contain(OccurrenceMonitor(expectedSelector.withUUID, None, Some(123)))
      monitorOf(OccurrenceSelector("Insecta|Mammalia", expectedWktString, expectedTraitSelector)) should be(Some(OccurrenceMonitor(OccurrenceSelector("Insecta|Mammalia", expectedWktString, expectedTraitSelector).withUUID, None, Some(123))))
      monitorOf(OccurrenceSelector("Aves|Mammalia", expectedWktString, expectedTraitSelector)) should be(Some(OccurrenceMonitor(OccurrenceSelector("Aves|Mammalia", expectedWktString, expectedTraitSelector).withUUID, None, Some(0))))
    }

    "return monitored occurrences" in {
      truncate
      session.execute("INSERT INTO effechecka.occurrence_first_added_search (source, added, id) " +
              "VALUES ('my source', '2016-01-04', 'some id')")

      session.execute("INSERT INTO effechecka.occurrence_search (source, id, taxonselector, wktstring, traitSelector) " +
              "VALUES ('my source', 'some id', 'Insecta|Mammalia', 'ENVELOPE(-150,-50,40,10)', 'bodyMass greaterThan 2.7 kg')")
      val expectedWktString: String = "ENVELOPE(-150,-50,40,10)"
      val expectedTraitSelector: String = "bodyMass greaterThan 2.7 kg"
      val expectedTaxonSelector: String = "Insecta|Mammalia"
      val occIter: Iterator[String] = monitoredOccurrencesFor("my source")
      occIter.next() should be("some id")
      occIter.hasNext should be(false)

      val monitors: Iterator[OccurrenceSelector] = monitorsFor(source = "my source", id = "some id")
      monitors.next() should be(OccurrenceSelector(expectedTaxonSelector, expectedWktString, expectedTraitSelector).withUUID)
      monitors.hasNext should be(false)
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

  def truncate: ResultSet = {
    session.execute("TRUNCATE effechecka.occurrence_first_added_search")
    session.execute("TRUNCATE effechecka.occurrence_search")
    session.execute("TRUNCATE effechecka.occurrence_collection_registry")
    session.execute("TRUNCATE effechecka.occurrence_collection")
  }

  def assertCountForAddedRange(addedBefore: Option[String], addedAfter: Option[String], addedDateString: String, expectedOccurrenceCount: Int): Unit = {
    val request: OccurrenceRequest = occurrenceQuery(addedBefore, addedAfter)
    session.execute("TRUNCATE effechecka.occurrence_collection")
    insertRequest(request.selector)

    session.execute("INSERT INTO effechecka.occurrence_collection (taxonselector, wktstring, traitSelector, taxon, lat, lng, start, end, id, added, source) " +
      "VALUES ('Insecta|Mammalia', 'ENVELOPE(-150,-50,40,10)', 'bodyMass greaterThan 2.7 kg', 'Aves|Donald duckus', 12.1, 11.1, 1234, 1235, 'http://record.url'," + addedDateString + ", 'http://archive.url')")
    val occurrenceCollection = occurrencesFor(request)
    occurrenceCollection.length should be(expectedOccurrenceCount)
  }

  def occurrenceQuery(addedBefore: Option[String], addedAfter: Option[String]): OccurrenceRequest = {
    OccurrenceRequest(OccurrenceSelector("Insecta|Mammalia", "ENVELOPE(-150,-50,40,10)", "bodyMass greaterThan 2.7 kg"), Some(2), DateTimeSelector(addedBefore, addedAfter))
  }
}