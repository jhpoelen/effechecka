package effechecka

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import akka.util.ByteString
import org.scalatest.{Matchers, WordSpecLike}

class OccurrenceCollectionFetcherHDFSSpec extends TestKit(ActorSystem("IntegrationTest"))
  with WordSpecLike with Matchers with OccurrenceCollectionFetcherHDFS with HDFSTestUtil {

  implicit val materializer = ActorMaterializer()(system)
  implicit val ec = system.dispatcher

  val expectedWktString: String = "ENVELOPE(-150,-50,40,10)"
  val expectedTraitSelector: String = ""
  val expectedSelector = SelectorParams("Animalia|Insecta", expectedWktString, expectedTraitSelector)
  val expectedSelectorUUID = SelectorUUID("55e4b0a0-bcd9-566f-99bc-357439011d85")
  val expectedMonitor = OccurrenceMonitor(expectedSelector, Some("ready"), Some(3997))

  val ducksAndFrogs = SelectorParams("Anas|Anura", "POLYGON ((-72.147216796875 41.492120839687786, -72.147216796875 43.11702412135048, -69.949951171875 43.11702412135048, -69.949951171875 41.492120839687786, -72.147216796875 41.492120839687786))").withUUID()
  val ducksAndFrogsMonitor = OccurrenceMonitor(ducksAndFrogs, Some("ready"), Some(239543))


  "HDFS" should {

    "have some expected test files" in {
      getClass.getResource("/hdfs-layout/occurrence/u0=55/u1=e4/u2=b0/uuid=55e4b0a0-bcd9-566f-99bc-357439011d85/occurrence.parquet") shouldNot be(null)
      getClass.getResource("/hdfs-layout/occurrence-summary/u0=06/u1=ac/u2=07/uuid=06ac07ec-7d99-521b-a768-04ab6013ab27") shouldNot be(null)
      getClass.getResource("/hdfs-layout/occurrence-summary/u0=05/u1=2f/u2=ec/uuid=052fec64-d7d8-5266-a4cd-119e3614831e") should be(null)
    }

    "access two records from occurrence collection" in {
      val request = OccurrenceRequest(ducksAndFrogs, Some(2))
      val occurrenceCollection = occurrencesFor(request)
      occurrenceCollection.hasNext shouldBe true
      val occ = occurrenceCollection.next
      occ.lat should be(42.37 +- 1e-2)
      occ.lng should be(-71.11 +- 1e-2)
      occ.taxon should be("Animalia|Chordata|Amphibia|Anura|Ranidae|Lithobates|palustris|Lithobates palustris")
      occ.start should be(-3176841600000L)
      occ.end should be(-3176841600000L)
      occ.added should be(1433203200000L)
      occ.id should be("MCZ:Herp:A-794")
      occ.source should be("idigbio")

      occurrenceCollection.take(2).length shouldBe 1
    }

    "access entire occurrence collection" in {
      val request = OccurrenceRequest(ducksAndFrogs, None)
      val occurrenceCollection = occurrencesFor(request)
      occurrenceCollection.size shouldBe 222
    }


    "occurrence selector with null status" in {
      monitors() should contain(ducksAndFrogsMonitor)
      monitorOf(ducksAndFrogs) should be(Some(ducksAndFrogsMonitor))
    }

    "return monitored occurrences by source and/or occurrence id" in {
      val probe = monitoredOccurrencesFor("inaturalist")
        .runWith(TestSink.probe[ByteString])
      probe
        .request(3)

      probe.expectNext() should be(ByteString.fromString("occurrenceId\tmonitorUUID"))
      val someNext = probe.expectNext()
      someNext should be(ByteString.fromString("\nhttp://www.inaturalist.org/observations/1053719\t"))
      probe.expectComplete()
    }

    "return monitored occurrences limit to 0" in {
      val probe = monitoredOccurrencesFor("inaturalist", occLimit = Some(0))
        .runWith(TestSink.probe[ByteString])
      probe
        .request(3)
      probe.expectNext(ByteString.fromString("occurrenceId\tmonitorUUID"))
      probe.expectComplete()

      val probe2 = monitoredOccurrencesFor("inaturalist", occLimit = Some(1))
        .runWith(TestSink.probe[ByteString])
      probe2
        .request(3)
      probe2.expectNext(ByteString.fromString("occurrenceId\tmonitorUUID"))
      probe2.expectNext()
      probe2.expectComplete()

    }

    "return status of monitor" in {
      statusOf(ducksAndFrogs) should be(Some("ready"))
      statusOf(SelectorParams("no|taxon", expectedWktString, "")) should be(None)
    }

  }


}
