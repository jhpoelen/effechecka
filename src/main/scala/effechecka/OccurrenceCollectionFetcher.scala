package effechecka

import org.effechecka.selector.{DateTimeSelector, OccurrenceSelector}

trait OccurrenceCollectionFetcher {
  def occurrencesFor(request: OccurrenceRequest): Iterator[Occurrence]

  def monitoredOccurrencesFor(source: String, added: DateTimeSelector, occLimit: Option[Int]): Iterator[String]

  def statusOf(selector: OccurrenceSelector): Option[String]

  def request(selector: OccurrenceSelector): String

  def requestAll(): String

  def monitors(): List[OccurrenceMonitor]

  def monitorOf(selector: OccurrenceSelector): Option[OccurrenceMonitor]

  def monitorsFor(source: String, id: String): Iterator[OccurrenceSelector]
}
