package effechecka

import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{FileIO, Sink}
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import akka.util.ByteString
import io.eels.FilePattern
import io.eels.component.parquet.{ParquetSink, ParquetSource}
import org.apache.hadoop.fs.Path
import org.scalatest.{Matchers, WordSpecLike}

class ChecklistFetcherHDFSSpec extends TestKit(ActorSystem("IntegrationTest"))
  with WordSpecLike with Matchers with ChecklistFetcherHDFS with HDFSTestUtil {

  implicit val materializer = ActorMaterializer()(system)
  implicit val ec = system.dispatcher

  private val reqSelector = SelectorParams("Animalia|Insecta", "ENVELOPE(-150,-50,40,10)", "")
  val req = ChecklistRequest(reqSelector, Some(2))
  val req5 = ChecklistRequest(reqSelector, Some(5))
  val reqNew = ChecklistRequest(SelectorParams("Aves|Mammalia", "ENVELOPE(-150,-50,40,10)", ""), Some(2))

  "HDFS" should {
    "have access to test resources" in {
      getClass.getResource("/hdfs-layout/checklist-summary/u0=55/u1=e4/u2=b0/uuid=55e4b0a0-bcd9-566f-99bc-357439011d85/summary.parquet") shouldNot be(null)
      getClass.getResource("/hdfs-layout/checklist/u0=55/u1=e4/u2=b0/uuid=55e4b0a0-bcd9-566f-99bc-357439011d85/checklist.parquet") shouldNot be(null)
    }

    "status existing" in {
      statusOf(req) shouldBe Some("ready")
    }

    "status non-existing" in {
      statusOf(reqNew) shouldBe None
    }


    "taxon name of empty taxon path" in {
      taxonNameFor(ChecklistItem("||||", 123)) shouldBe ""
    }

    "taxon name of non-empty taxon path" in {
      taxonNameFor(ChecklistItem("|some|none|empty|", 123)) shouldBe "empty"
    }

    "return items" in {
      val checklist = itemsFor(req).toSeq
      checklist should contain(ChecklistItem("Animalia|Chordata|Aves|Passeriformes|Paridae|Poecile|atricapillus|Poecile atricapillus (Linnaeus, 1766)", 126643))
      checklist.length shouldBe 2
    }

    "return source" in {
      println(ByteString(116, 97, 120, 111, 110, 78, 97, 109, 101, 9, 116, 97, 120, 111, 110, 80, 97, 116, 104, 9, 114, 101, 99, 111, 114, 100, 67, 111, 117, 110, 116).utf8String)
      println(ByteString.fromString("bla").utf8String)
      val probe = tsvFor(req)
        .runWith(TestSink.probe[ByteString])
      probe
        .request(3)
        .expectNext(ByteString.fromString("taxonName\ttaxonPath\trecordCount"))

      val items = List(probe.expectNext().utf8String, probe.expectNext().utf8String)
      items should contain("\nPoecile atricapillus (Linnaeus, 1766)\tAnimalia|Chordata|Aves|Passeriformes|Paridae|Poecile|atricapillus|Poecile atricapillus (Linnaeus, 1766)\t126643")
      items should contain("\nTurdus migratorius Linnaeus, 1766\tAnimalia|Chordata|Aves|Passeriformes|Turdidae|Turdus|migratorius|Turdus migratorius Linnaeus, 1766\t114323")
      probe.expectComplete()
    }

    "return 5 items" in {
      val checklist = itemsFor(req5).toSeq
      checklist.length shouldBe 5
    }

    "return no items" in {
      val checklist = itemsFor(reqNew).toSeq
      checklist shouldNot contain(ChecklistItem("Animalia|Chordata|Aves|Passeriformes|Paridae|Poecile|atricapillus|Poecile atricapillus (Linnaeus, 1766)", 126643))
    }
  }
}