package effechecka

import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{FileIO, Sink}
import akka.testkit.TestKit
import io.eels.FilePattern
import org.apache.hadoop.fs.Path
import org.scalatest.{Matchers, WordSpecLike}
import io.eels.component.csv.CsvSource
import io.eels.component.parquet.{ParquetSink, ParquetSource}
import org.effechecka.selector.OccurrenceSelector

class ChecklistFetcherHDFSSpec extends TestKit(ActorSystem("IntegrationTest"))
  with WordSpecLike with Matchers with ChecklistFetcherHDFS with HDFSTestUtil {

  implicit val materializer = ActorMaterializer()(system)
  implicit val ec = system.dispatcher

  private val reqSelector = OccurrenceSelector("Animalia|Insecta", "ENVELOPE(-150,-50,40,10)", "")
  val req = ChecklistRequest(reqSelector, 2)
  val req5 = ChecklistRequest(reqSelector, 5)
  val reqNew = ChecklistRequest(OccurrenceSelector("Aves|Mammalia", "ENVELOPE(-150,-50,40,10)", ""), 2)

  "HDFS" should {
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
      val pathFull = Paths.get(baseDir + "/" + pathForRequest + "/spark.parquet")
      val pattern = FilePattern(pathFull + "/*").withFilter(_.getName.endsWith(".parquet"))
      val firstTaxonNameCombo = ParquetSource(pattern).toFrame().collect().map(_.values).head.head
      firstTaxonNameCombo shouldBe "Poecile atricapillus (Linnaeus, 1766)"
    }

    "create path for selector" in {
      val pathForRequest = pathForChecklist(req.selector)
      pathForRequest shouldBe "occurrencesForMonitor/55/e4/b0/55e4b0a0-bcd9-566f-99bc-357439011d85/checklist"

      val pathFull = Paths.get(baseDir + "/" + pathForRequest + "/20.tsv/checklist20.tsv")
      FileIO.fromPath(pathFull)
        .to(Sink.ignore)

      val resourcesDir = pathFull.getParent

      val source = CsvSource(pathFull).withDelimiter('\t')

      val output1 = new Path("target/pq/output1.pq")
      source.toFrame()
        .filter({ row =>
          println(s"filter1 [${row.get("taxonName")}]")
          row.get("taxonName") == "Poecile atricapillus (Linnaeus, 1766)"
        })
        .save(ParquetSink(output1).withOverwrite(true))

      val output2 = new Path("target/pq/output2.pq")
      source.toFrame()
        .filter({ row =>
          println(s"filter2 [${row.get("taxonName")}]")
          row.get("taxonName") != "Poecile atricapillus (Linnaeus, 1766)"
        })
        .save(ParquetSink(output2).withOverwrite(true))

      val resourcePath = fs.resolvePath(output2.getParent)

      val pattern = FilePattern(resourcePath + "/*").withFilter(
        path => {
          true
        }
      )
      val firstTaxonNameCombo = ParquetSource(output1).toFrame().map(r => {
        println(s"map1 [${r.get("taxonName")}]")
        r
      }).toSeq().map {
        row => {
          println(s"map2 [${row.get("taxonName")}]")
          row.values
        }
      }.head.head
      firstTaxonNameCombo shouldBe "Poecile atricapillus (Linnaeus, 1766)"
      val firstTaxonName = ParquetSource(output2)
        .toFrame()
        .map(r => {
          println(s"map21 [${r.get("taxonName")}]")
          r
        }).rows().iterator.flatMap {
        row => {
          println(s"map22 [${row.get("taxonName")}]")
          row.values.map(_.toString)
        }
      }.mkString(" ")
      firstTaxonName shouldNot contain("Poecile atricapillus (Linnaeus, 1766)")
    }
  }

}
