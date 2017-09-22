package effechecka

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, SourceShape}
import akka.stream.scaladsl.{Concat, Flow, GraphDSL, Sink, Source}
import akka.util.ByteString
import com.typesafe.config.Config
import io.eels.Row
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._


trait ChecklistFetcherHDFS
  extends ChecklistFetcher
    with ParquetReaderIterator
    with HDFSUtil {

  implicit def config: Config

  protected implicit val configHadoop: Configuration
  protected implicit val fs: FileSystem

  implicit val materializer: ActorMaterializer


  def tsvFor(checklist: ChecklistRequest): Source[ByteString, NotUsed] = {
    val checklistGraph = GraphDSL.create(toSourceShape(checklist = checklist)) { implicit builder =>
      (checklist) =>
        import GraphDSL.Implicits._
        val toByteString = Flow[ChecklistItem]
          .map(item => {
            val taxonName = taxonNameFor(item)
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

  protected def taxonNameFor(item: ChecklistItem): String = {
    item.taxon.split("""\|""").filter(_.trim.nonEmpty).reverse.headOption.getOrElse("")
  }

  private def sourceForItems(checklist: ChecklistRequest): Source[ChecklistItem, NotUsed] = {
    Source.fromGraph(toSourceShape(checklist))
  }

  private def toSourceShape(checklist: ChecklistRequest) = {
    GraphDSL.create(new ParquetReaderSourceShape(checklistPath(checklist, "checklist/", ""), checklist.limit)) { implicit builder =>
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
    val runWith = sourceForItems(checklist).runWith(Sink.seq)
    Await.result(runWith, 30.second).iterator
  }

  private def checklistExists(checklist: ChecklistRequest) = {
    val runWith = sourceForItems(checklist).runWith(Sink.seq)
    Await.result(runWith, 30.second).iterator.nonEmpty
  }

  private def checklistPath(checklist: ChecklistRequest, prefix: String, suffix: String) = {
    patternFor(prefix + pathForChecklist(checklist.selector) + suffix)
  }

  def pathForChecklist(occurrenceSelector: Selector): String = {
    pathForSelector(occurrenceSelector)
  }

  def statusOf(checklist: ChecklistRequest): Option[String] = {
    if (checklistExists(checklist)) Some("ready") else None
  }


}

