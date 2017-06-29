package effechecka

import org.effechecka.selector.{DateTimeSelector, OccurrenceSelector}

case class OccurrenceRequest(selector: OccurrenceSelector, limit: Option[Int] = None, added: DateTimeSelector = DateTimeSelector())
case class Occurrence(taxon: String, lat: Double, lng: Double, start: Long, end: Long, id: String, added: Long, source: String)
case class OccurrenceCollection(selector: OccurrenceSelector, status: Option[String], items: List[Occurrence] = List())
case class OccurrenceMonitor(selector: OccurrenceSelector, status: Option[String], recordCount: Option[Int])
