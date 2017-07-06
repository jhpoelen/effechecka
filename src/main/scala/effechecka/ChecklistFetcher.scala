package effechecka

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.util.ByteString

trait ChecklistFetcher {
  def itemsFor(checklist: ChecklistRequest): Iterator[ChecklistItem]
  def statusOf(checklist: ChecklistRequest): Option[String]
  def request(checklist: ChecklistRequest): String
  def tsvSourceFor(checklist: ChecklistRequest): Source[ByteString, NotUsed]
}
