package effechecka

import com.typesafe.config.Config
import io.eels.{Frame, Row}
import io.eels.component.parquet.ParquetSource
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.effechecka.selector.{DateTimeSelector, OccurrenceSelector, UuidUtils}

trait OccurrenceCollectionFetcherHDFS
  extends OccurrenceCollectionFetcher
  with SparkSubmitter with HDFSUtil {

  implicit def config: Config

  protected implicit val configHadoop: Configuration
  protected implicit val fs: FileSystem

  def occurrencesFor(ocRequest: OccurrenceRequest): Iterator[Occurrence] = {
    val selector = ocRequest.selector
    val pathBase = s"occurrence/${pathForSelector(selector)}"
    patternFor(pathBase) match {
      case Some(pathPattern) =>
        val source = ParquetSource(pathPattern.withFilter(pathFilterWithDateRange(ocRequest.added)))
        if (source.parts().isEmpty) Iterator() else {
          val frame = source.toFrame()
          val iterator: Iterator[Row] = frameLimited(frame, ocRequest.limit)
          iterator
            .map(row => {
              val startDate = java.lang.Long.parseLong(row.get("start").toString)
              Occurrence(taxon = row.get("taxon").toString,
                lat = java.lang.Double.parseDouble(row.get("lat").toString),
                lng = java.lang.Double.parseDouble(row.get("lng").toString),
                start = startDate,
                end = startDate,
                id = row.get("id").toString,
                added = java.lang.Long.parseLong(row.get("added").toString),
                source = row.get("source").toString
              )
            })
        }
      case None => Iterator()
    }
  }

  private def frameLimited(frame: Frame, limit: Option[Int]) = {
    val iterator = limit match {
      case Some(aLimit) => frame.rows().iterator.take(aLimit)
      case _ => frame.rows.iterator
    }
    iterator
  }

  def monitoredOccurrencesFor(source: String, added: DateTimeSelector = DateTimeSelector(), occLimit: Option[Int] = None): Iterator[(String, Option[String])] = {
    patternFor(s"source-of-monitored-occurrence/source=$source") match {
      case Some(path) =>
        val source = ParquetSource(path)
          if (source.parts().isEmpty) Iterator() else {
            val iterator: Iterator[Row] = frameLimited(source.toFrame(), occLimit)
            iterator
              .map(row => {
                val values = row.values
                (values.head.toString, if (values.size > 1) Some(values(1).toString) else None)
              })
          }
      case None => Iterator()
    }
  }


  val monitor = OccurrenceMonitor(selector = OccurrenceSelector("Animalia|Insecta", "ENVELOPE(-150,-50,40,10)", ""), status = None, recordCount = Some(123))

  def monitors(): List[OccurrenceMonitor] = {
    patternFor(s"occurrence-summary") match {
      case Some(path) =>
        val source = ParquetSource(path)
        if (source.parts().isEmpty) List() else {
          val monitors = source
            .toFrame().rows().iterator
          monitors.map(row => {
            val statusOpt = row.get("status").toString
            val recordOpt = Some(Integer.parseInt(row.get("itemCount").toString))
            OccurrenceMonitor(selectorFromRow(row), Some(statusOpt), recordOpt)
          }).toList
        }
      case None => List()
    }
  }

  private def selectorFromRow(row: Row): OccurrenceSelector = {
    val selector = OccurrenceSelector(taxonSelector = row.get("taxonSelector").toString, traitSelector = row.get("traitSelector").toString, wktString = row.get("wktString").toString)
    selector.withUUID
  }

  def monitorOf(selector: OccurrenceSelector): Option[OccurrenceMonitor] = {
    patternFor(s"occurrence-summary/${pathForSelector(selector)}") match {
      case Some(path) =>
        val source = ParquetSource(path)
        if (source.parts().isEmpty) None else {
          val monitors = source.toFrame().rows().iterator
          if (monitors.isEmpty) None else {
            monitors.map(row => {
              val statusOpt = row.get("status").toString
              val recordOpt = Some(Integer.parseInt(row.get("itemCount").toString))
              val selectorWithUUID = selector.withUUID
              OccurrenceMonitor(selectorWithUUID, Some(statusOpt), recordOpt)
            }).take(1).toList.headOption
          }
        }
      case None => None
    }
  }

  def monitorsFor(source: String, id: String): Iterator[OccurrenceSelector] = {
    patternFor(s"monitor-of-occurrence/${pathForUUID(UuidUtils.generator.generate(id))}/source=$source") match {
      case Some(path) =>
        val source = ParquetSource(path)
          if (source.parts().isEmpty) Iterator() else {
            source.toFrame().rows().iterator.map(selectorFromRow)
          }
      case None => Iterator()
    }
  }


  def request(selector: OccurrenceSelector): String = {
    statusOf(selector) match {
      case Some("ready") => "ready"
      case _ =>
        submitOccurrenceCollectionRequest(selector, persistence = "hdfs")
        "requested"
    }
  }

  def requestAll(): String = {
    submitOccurrenceCollectionsRefreshRequest()
    "all requested"
  }

  def statusOf(selector: OccurrenceSelector): Option[String] = {
    monitorOf(selector) match {
      case Some(aMonitor) => aMonitor.status
      case None => None
    }
  }

}

