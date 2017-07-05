package effechecka

import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{FileIO, Sink}
import akka.testkit.TestKit
import io.eels.FilePattern
import org.apache.hadoop.fs.Path
import org.scalatest.{Matchers, WordSpecLike}
import io.eels.component.parquet.{ParquetSink, ParquetSource}
import org.effechecka.selector.OccurrenceSelector

class ChecklistFetcherHDFSSpec extends TestKit(ActorSystem("IntegrationTest"))
  with WordSpecLike with Matchers with ChecklistFetcherHDFS with HDFSTestUtil {

  implicit val materializer = ActorMaterializer()(system)
  implicit val ec = system.dispatcher

  private val reqSelector = OccurrenceSelector("Animalia|Insecta", "ENVELOPE(-150,-50,40,10)", "")
  val req = ChecklistRequest(reqSelector, Some(2))
  val req5 = ChecklistRequest(reqSelector, Some(5))
  val reqNew = ChecklistRequest(OccurrenceSelector("Aves|Mammalia", "ENVELOPE(-150,-50,40,10)", ""), Some(2))

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

    "request a checklist already exists" in {
      request(req) shouldBe "ready"
    }

//    "request a checklist new" in {
//      request(reqNew) shouldBe "requested"
//    }

    "return items" in {
      val checklist = itemsFor(req).toSeq
      checklist should contain(ChecklistItem("Animalia|Chordata|Aves|Passeriformes|Paridae|Poecile|atricapillus|Poecile atricapillus (Linnaeus, 1766)",126643))
      checklist.length shouldBe 2
    }

    "return 5 items" in {
      val checklist = itemsFor(req5).toSeq
      checklist.length shouldBe 5
    }

    "return no items" in {
      val checklist = itemsFor(reqNew).toSeq
      checklist shouldNot contain(ChecklistItem("Animalia|Chordata|Aves|Passeriformes|Paridae|Poecile|atricapillus|Poecile atricapillus (Linnaeus, 1766)",126643))
    }

    "read parquet by spark" in {
      val pathForRequest = pathForChecklist(req.selector)
      val pathFull = Paths.get(baseDir + "/checklist/" + pathForRequest + "/checklist.parquet")
      val pattern = FilePattern(pathFull + "/*").withFilter(_.getName.endsWith(".parquet"))
      val firstTaxonNameCombo = ParquetSource(pattern).toFrame().collect().map(_.values).head.head
      firstTaxonNameCombo shouldBe "Poecile atricapillus (Linnaeus, 1766)"
    }

    "create path for selector" in {
      val pathForRequest = pathForChecklist(req.selector)
      pathForRequest shouldBe "u0=55/u1=e4/u2=b0/uuid=55e4b0a0-bcd9-566f-99bc-357439011d85"

      val pathFull = Paths.get(baseDir + "/checklist/" + pathForRequest + "/checklist.parquet")
      FileIO.fromPath(pathFull)
        .to(Sink.ignore)

      val resourcesDir = pathFull.getParent

      val source = ParquetSource(pathFull)

      val output1 = new Path("target/pq/output1.pq")
      source.toFrame()
        .filter({ row =>
          row.get("taxonName") == "Poecile atricapillus (Linnaeus, 1766)"
        })
        .save(ParquetSink(output1).withOverwrite(true))

      val output2 = new Path("target/pq/output2.pq")
      source.toFrame()
        .filter({ row =>
          row.get("taxonName") != "Poecile atricapillus (Linnaeus, 1766)"
        })
        .save(ParquetSink(output2).withOverwrite(true))

      val resourcePath = fs.resolvePath(output2.getParent)

      val pattern = FilePattern(resourcePath + "/*").withFilter(
        path => {
          true
        }
      )
      val firstTaxonNameCombo = ParquetSource(output1).toFrame().toSeq().map {
        row => {
          row.values
        }
      }.head.head
      firstTaxonNameCombo shouldBe "Poecile atricapillus (Linnaeus, 1766)"
      val firstTaxonName = ParquetSource(output2)
        .toFrame()
        .rows().iterator.flatMap {
        row => {
          row.values.map(_.toString)
        }
      }.mkString(" ")
      firstTaxonName shouldNot contain("Poecile atricapillus (Linnaeus, 1766)")
    }
  }

}
