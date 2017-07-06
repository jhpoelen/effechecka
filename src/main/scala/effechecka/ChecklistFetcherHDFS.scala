package effechecka

import com.typesafe.config.Config
import io.eels.FilePattern
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.effechecka.selector.OccurrenceSelector


trait ChecklistFetcherHDFS
  extends ChecklistFetcher
  with SparkSubmitter
  with EelRowIterator
  with HDFSUtil {

  implicit def config: Config

  protected implicit val configHadoop: Configuration
  protected implicit val fs: FileSystem


  def itemsFor(checklist: ChecklistRequest): Iterator[ChecklistItem] = {
      getRows(checklistPath(checklist, "checklist/", ""), checklist.limit)
      .map(row => ChecklistItem(row.get("taxonPath").toString, Integer.parseInt(row.get("recordCount").toString))).iterator
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
    getRows(checklistPath(checklist, "checklist-summary/", ""), Some(1)).iterator.nonEmpty
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

