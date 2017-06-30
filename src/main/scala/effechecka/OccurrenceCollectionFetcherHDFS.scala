package effechecka

import com.typesafe.config.Config
import io.eels.{FilePattern, Row}
import io.eels.component.parquet.ParquetSource
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.effechecka.selector.{DateTimeSelector, OccurrenceSelector, UuidUtils}

trait OccurrenceCollectionFetcherHDFS extends OccurrenceCollectionFetcher
  with SparkSubmitter with HDFSUtil {

  implicit def config: Config

  protected implicit val configHadoop: Configuration = new Configuration()
  protected implicit val fs: FileSystem = FileSystem.get(configHadoop)

  def occurrencesFor(ocRequest: OccurrenceRequest): Iterator[Occurrence] = {
    val selector = ocRequest.selector
    val pathBase = s"${pathForSelector(selector)}/occurrence/spark.parquet"
    patternFor(pathBase) match {
      case Some(pathPattern) =>
        val source = ParquetSource(pathPattern.withFilter(pathFilterWithDateRange(ocRequest.added)))
        if (source.parts().isEmpty) Iterator() else {
          val frame = source.toFrame()
          val iterator = ocRequest.limit match {
            case Some(aLimit) => frame.rows().iterator.take(aLimit)
            case _ => frame.rows.iterator
          }
          iterator
            .map(row => {
              val startDate = java.lang.Long.parseLong(row.get("eventStartDate").toString)
              Occurrence(taxon = row.get("taxonPath").toString,
                lat = java.lang.Double.parseDouble(row.get("lat").toString),
                lng = java.lang.Double.parseDouble(row.get("lng").toString),
                start = startDate,
                end = startDate,
                id = row.get("occurrenceId").toString,
                added = java.lang.Long.parseLong(row.get("firstAddedDate").toString),
                source = row.get("source").toString
              )
            })
        }
      case None => Iterator()
    }
  }

  def monitoredOccurrencesFor(source: String, added: DateTimeSelector = DateTimeSelector(), occLimit: Option[Int] = None): Iterator[String] = {
    patternFor(s"occurrencesForSource/source=$source/ids.parquet") match {
      case Some(path) =>
        val source = ParquetSource(path)
          if (source.parts().isEmpty) Iterator() else {
            source.toFrame().rows().iterator
              .map(row => {
                row.get(0).toString
              })
          }
      case None => Iterator()
    }
  }


  val monitor = OccurrenceMonitor(selector = OccurrenceSelector("Animalia|Insecta", "ENVELOPE(-150,-50,40,10)", ""), status = None, recordCount = Some(123))

  def monitors(): List[OccurrenceMonitor] = {
    def includeSummariesOnly(path: Path) = FilePattern(path + "/*")
      .withFilter({ x => {
        x.toUri.toString.contains("occurrence/summary.parquet")
      }
      })

    patternFor(s"occurrencesForMonitor/", includeSummariesOnly) match {
      case Some(path) =>
        val source = ParquetSource(path)
        if (source.parts().isEmpty) List() else {
          val monitors = source
            .toFrame().rows().iterator
          monitors.map(row => {
            val statusOpt = row.get("status").toString
            val recordOpt = Some(Integer.parseInt(row.get("count").toString))
            OccurrenceMonitor(selectorFromRow(row), Some(statusOpt), recordOpt)
          }).toList
        }
      case None => List()
    }
  }

  private def selectorFromRow(row: Row): OccurrenceSelector = {
    val selector = OccurrenceSelector(taxonSelector = row.get("taxonSelector").toString, traitSelector = row.get("traitSelector").toString, wktString = row.get("wktString").toString)
    selector.copy(uuid = Some(row.get("uuid").toString))
  }

  def monitorOf(selector: OccurrenceSelector): Option[OccurrenceMonitor] = {
    patternFor(s"${pathForSelector(selector)}/occurrence/summary.parquet") match {
      case Some(path) =>
        val source = ParquetSource(path)
        if (source.parts().isEmpty) None else {
          val monitors = source.toFrame().rows().iterator
          if (monitors.isEmpty) None else {
            monitors.map(row => {
              val statusOpt = row.get("status").toString
              val recordOpt = Some(Integer.parseInt(row.get("count").toString))
              val selectorWithUUID = selector.copy(uuid = Some(row.get("uuid").toString))
              OccurrenceMonitor(selectorWithUUID, Some(statusOpt), recordOpt)
            }).take(1).toList.headOption
          }
        }
      case None => None
    }
  }

  def monitorsFor(source: String, id: String): Iterator[OccurrenceSelector] = {
    patternFor(s"monitorsForOccurrence/${pathForUUID(UuidUtils.generator.generate(id))}/source=$source") match {
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
      case _ => {
        submitOccurrenceCollectionRequest(selector, persistence = "hdfs")
        "requested"
      }
    }
  }

  def requestAll(): String = {
    "all requested"
  }

  def statusOf(selector: OccurrenceSelector): Option[String] = {
    monitorOf(selector) match {
      case Some(aMonitor) => aMonitor.status
      case None => None
    }
  }

}

