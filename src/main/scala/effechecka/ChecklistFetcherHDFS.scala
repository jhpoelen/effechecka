package effechecka

import com.typesafe.config.Config
import io.eels.component.parquet.ParquetSource
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.effechecka.selector.OccurrenceSelector


trait ChecklistFetcherHDFS extends ChecklistFetcher with SparkSubmitter with HDFSUtil {

  implicit def config: Config

  protected implicit val configHadoop: Configuration = new Configuration()
  protected implicit val fs: FileSystem = FileSystem.get(configHadoop)

  def itemsFor(checklist: ChecklistRequest): Iterator[ChecklistItem] = {
    checklistPath(checklist, "/spark.parquet") match {
      case Some(path) =>
        val source = ParquetSource(path)
          if (source.parts().isEmpty) Iterator() else {
            source.toFrame().rows().iterator
              .take(checklist.limit)
              .map(row => ChecklistItem(row.get("taxonPath").toString, Integer.parseInt(row.get("recordCount").toString)))
          }
      case None => Iterator() // should really be None
    }
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
    val pathChecklist = checklistPath(checklist, "/summary.parquet")
    pathChecklist match {
      case Some(path) => path.toPaths().nonEmpty
      case None => false
    }
  }

  private def checklistPath(checklist: ChecklistRequest, part: String) = {
    patternFor(pathForChecklist(checklist.selector) + part)
  }


  def statusOf(checklist: ChecklistRequest): Option[String] = {
    if (checklistExists(checklist)) Some("ready") else None
  }

  def pathForChecklist(occurrenceSelector: OccurrenceSelector): String = {
    pathForSelector(occurrenceSelector) + "/checklist"
  }


}

