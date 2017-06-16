package effechecka

import akka.stream.scaladsl.{FileIO, Sink}
import org.scalatest.{Matchers, WordSpec}

class ChecklistFetcherHDFSSpec extends WordSpec with Matchers with ChecklistFetcherHDFS {
  val req = ChecklistRequest(OccurrenceSelector("Animalia|Insecta", "ENVELOPE(-150,-50,40,10)", ""), 2)

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

    "create path for selector" in {
      val pathForRequest = pathFor(req.selector)
      pathForRequest shouldBe "occurrencesForMonitor/55/e4/b0/55e4b0a0-bcd9-566f-99bc-357439011d85/checklist/"

      FileIO.fromFile(new java.io.File(getClass.getResource("/hdfs-layout/" + pathForRequest + "20.tsv/checklist20.tsv").toURI))
        .to(Sink.ignore)
    }
  }

}
