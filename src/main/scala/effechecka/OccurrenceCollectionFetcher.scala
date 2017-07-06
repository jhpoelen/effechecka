package effechecka

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.util.ByteString
import org.effechecka.selector.{DateTimeSelector, OccurrenceSelector}

trait OccurrenceCollectionFetcher {
  def occurrencesTsvFor(request: OccurrenceRequest): Source[ByteString, NotUsed]
  def occurrencesFor(request: OccurrenceRequest): Iterator[Occurrence]

  def monitoredOccurrencesFor(source: String, added: DateTimeSelector, occLimit: Option[Int]): Source[ByteString, NotUsed]

  def statusOf(selector: OccurrenceSelector): Option[String]

  def request(selector: OccurrenceSelector): String

  def requestAll(): String

  def monitors(): List[OccurrenceMonitor]

  def monitorOf(selector: OccurrenceSelector): Option[OccurrenceMonitor]

}
