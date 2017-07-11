package effechecka

import org.effechecka.selector.DateTimeSelector

case class OccurrenceRequest(selector: Selector, limit: Option[Int] = None, added: DateTimeSelector = DateTimeSelector())
case class Occurrence(taxon: String, lat: Double, lng: Double, start: Long, end: Long, id: String, added: Long, source: String)
case class OccurrenceCollection(selector: Selector, status: Option[String], items: List[Occurrence] = List())
case class OccurrenceMonitor(selector: Selector, status: Option[String], recordCount: Option[Int])
