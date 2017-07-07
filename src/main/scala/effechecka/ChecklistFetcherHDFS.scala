package effechecka

import akka.NotUsed
import akka.stream.SourceShape
import akka.stream.scaladsl.{Concat, Flow, GraphDSL, Sink, Source}
import akka.util.ByteString
import com.typesafe.config.Config
import io.eels.Row
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.effechecka.selector.OccurrenceSelector

import scala.concurrent.Await
import scala.concurrent.duration._


trait ChecklistFetcherHDFS
  extends ChecklistFetcher
    with SparkSubmitter
    with ParquetIterator
    with HDFSUtil {

  implicit def config: Config

  protected implicit val configHadoop: Configuration
  protected implicit val fs: FileSystem

  def tsvFor(checklist: ChecklistRequest): Source[ByteString, NotUsed] = {
    val checklistGraph = GraphDSL.create(toSourceShape(checklist = checklist)) { implicit builder =>
      (checklist) =>
        import GraphDSL.Implicits._
        val toByteString = Flow[ChecklistItem]
          .map(item => {
            val taxonName = item.taxon.split("""\|""").reverse.head
            ByteString(s"\n$taxonName\t${item.taxon}\t${item.recordcount}")
          })
        val out = builder.add(toByteString)
        checklist ~> out
        SourceShape(out.out)
    }
    val occurrenceSource = Source.fromGraph(checklistGraph)
    val header = Source.single[ByteString](ByteString(Seq("taxonName", "taxonPath", "recordCount").mkString("\t")))
    Source.combine(header, occurrenceSource)(Concat[ByteString])
  }

  private def itemsFor2(checklist: ChecklistRequest): Source[ChecklistItem, NotUsed] = {
    Source.fromGraph(toSourceShape(checklist))
  }

  private def toSourceShape(checklist: ChecklistRequest) = {
    GraphDSL.create(new ParquetSourceShape(checklistPath(checklist, "checklist/", ""), checklist.limit)) { implicit builder =>
      (checklist) =>
        import GraphDSL.Implicits._
        val toItems = Flow[Row]
          .map(row => ChecklistItem(row.get("taxonPath").toString, Integer.parseInt(row.get("recordCount").toString)))
        val out = builder.add(toItems)
        checklist ~> out
        SourceShape(out.out)
    }
  }

  def itemsFor(checklist: ChecklistRequest): Iterator[ChecklistItem] = {
    val runWith = itemsFor2(checklist).runWith(Sink.seq)
    Await.result(runWith, 30.second).iterator
  }

  def request(checklist: ChecklistRequest): String = {
    if (checklistExists(checklist)) {
      "ready"
    } else {
      submitChecklistRequest(checklist, "hdfs")
      "requested"
    }
  }

  private def checklistExists(checklist: ChecklistRequest) = {
    val runWith = itemsFor2(checklist).runWith(Sink.seq)
    Await.result(runWith, 30.second).iterator.nonEmpty
  }

  private def checklistPath(checklist: ChecklistRequest, prefix: String, suffix: String) = {
    patternFor(prefix + pathForChecklist(checklist.selector) + suffix)
  }

  def pathForChecklist(occurrenceSelector: OccurrenceSelector): String = {
    pathForSelector(occurrenceSelector)
  }

  def statusOf(checklist: ChecklistRequest): Option[String] = {
    if (checklistExists(checklist)) Some("ready") else None
  }


}

