package effechecka

import java.nio.file.Paths

import akka.stream.scaladsl.{FileIO, Sink}
import com.typesafe.config.Config
import io.eels.FilePattern
import io.eels.component.csv.CsvSource
import io.eels.component.parquet.ParquetSource
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}


trait ChecklistFetcherHDFS extends ChecklistFetcher {

  implicit def config: Config

  protected implicit val configHadoop = new Configuration()
  protected implicit val fs = FileSystem.get(configHadoop)

  def itemsFor(checklist: ChecklistRequest): Iterator[ChecklistItem] = {
    checklistPath(checklist, "spark.parquet") match {
      case Some(path) =>
        ParquetSource(path)
          .toFrame().rows().iterator
          .map(row => ChecklistItem(row.get("taxonPath").toString, Integer.parseInt(row.get("recordCount").toString)))
      case None => Iterator() // should really be None
    }
  }

  def request(checklist: ChecklistRequest): String = {
    if (checklistExists(checklist)) "ready" else "requested"
  }

  private def checklistExists(checklist: ChecklistRequest) = {
    val pathChecklist = checklistPath(checklist, "summary.tsv")
    pathChecklist match {
      case Some(path) => path.toPaths().nonEmpty
      case None => false
    }
  }

  private def checklistPath(checklist: ChecklistRequest, part: String) = {
    val pathForRequest = pathFor(checklist.selector)
    val pathString = baseDir + "/" + pathForRequest + part
    val pathFull = Paths.get(pathString)
    val path = new Path(pathFull.getParent.toAbsolutePath.toString)
    if (fs.exists(path)) {
      val resourcePath = fs.resolvePath(path)
      Some(FilePattern(resourcePath + "/*"))
    } else {
      None
    }
  }

  def statusOf(checklist: ChecklistRequest): Option[String] = {
    if (checklistExists(checklist)) Some("ready") else None
  }

  def pathFor(occurrenceSelector: OccurrenceSelector) = {
    val uuid = UuidUtils.uuidFor(occurrenceSelector)
    val f0 = uuid.toString.substring(0, 2)
    val f1 = uuid.toString.substring(2, 4)
    val f2 = uuid.toString.substring(4, 6)
    s"occurrencesForMonitor/$f0/$f1/$f2/$uuid/checklist/"
  }

  protected def baseDir = {
    config.getString("effechecka.monitor.dir")
  }


}

