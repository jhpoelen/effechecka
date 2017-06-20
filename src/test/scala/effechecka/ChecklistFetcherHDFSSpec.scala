package effechecka

import java.nio.file.Paths

import akka.stream.scaladsl.{FileIO, Sink}
import io.eels.FilePattern
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import org.scalatest.{Matchers, WordSpec}
import io.eels.component.csv.CsvSource
import io.eels.schema._
import io.eels.component.parquet.{ParquetSink, ParquetSource}

class ChecklistFetcherHDFSSpec extends WordSpec with Matchers with ChecklistFetcherHDFS {
  val req = ChecklistRequest(OccurrenceSelector("Animalia|Insecta", "ENVELOPE(-150,-50,40,10)", ""), 2)

  private implicit val conf = new Configuration()
  private implicit val fs = FileSystem.get(conf)

  "HDFS" should {
    "produce some status" in {
      statusOf(req) shouldBe Some("unknown")
    }

    "request a checklist" in {
      request(req) shouldBe "unknown"
    }

    "produce a wellformed status query" in {
      val checklist = itemsFor(req)
      checklist should contain(ChecklistItem("a|name", 1234))
    }

    "read parquet by spark" in {
      val pathForRequest = pathFor(req.selector)
      val pathFull = Paths.get(getClass.getResource("/hdfs-layout/" + pathForRequest + "spark.parquet").getFile)
      val pattern = FilePattern(pathFull + "/*").withFilter(_.getName.endsWith(".parquet"))
      val firstTaxonNameCombo = ParquetSource(pattern).toFrame().collect().map(_.values).head.head
      firstTaxonNameCombo shouldBe "Poecile atricapillus (Linnaeus, 1766)"

    }

    "create path for selector" in {
      val pathForRequest = pathFor(req.selector)
      pathForRequest shouldBe "occurrencesForMonitor/55/e4/b0/55e4b0a0-bcd9-566f-99bc-357439011d85/checklist/"

      val pathFull = Paths.get(getClass.getResource("/hdfs-layout/" + pathForRequest + "20.tsv/checklist20.tsv").getFile)
      FileIO.fromPath(pathFull)
        .to(Sink.ignore)

      val resourcesDir = pathFull.getParent

      val source = CsvSource(pathFull).withDelimiter('\t')

      val output1 = new Path("target/pq/output1.pq")
      source.toFrame()
        .filter(_.get("taxonName") == "Poecile atricapillus (Linnaeus, 1766)")
        .save(ParquetSink(output1).withOverwrite(true))

      val output2 = new Path("target/pq/output2.pq")
      source.toFrame()
        .filter(_.get("taxonName") != "Poecile atricapillus (Linnaeus, 1766)")
        .save(ParquetSink(output2).withOverwrite(true))

      val resourcePath = fs.resolvePath(output2.getParent)

      val pattern = FilePattern(resourcePath + "/*").withFilter(
        path => {
          true
        }
      )
      val firstTaxonNameCombo = ParquetSource(output1).toFrame().collect().map(_.values).head.head
      firstTaxonNameCombo shouldBe "Poecile atricapillus (Linnaeus, 1766)"
      val firstTaxonName = ParquetSource(output2).toFrame().collect().map(_.values).head.head
      firstTaxonName shouldNot be("Poecile atricapillus (Linnaeus, 1766)")


    }
  }

}
