package effechecka

case class OccurrenceSelector(taxonSelector: String, wktString: String, traitSelector: String)

case class OccurrenceCollectionRequest(selector: OccurrenceSelector, limit: Int, addedBefore: Option[String] = None, addedAfter: Option[String] = None)
case class Occurrence(taxon: String, lat: Double, lng: Double, start: Long, end: Long, id: String, added: Long, source: String)
case class OccurrenceCollection(selector: OccurrenceSelector, status: String, items: List[Occurrence])
case class OccurrenceMonitor(selector: OccurrenceSelector, status: String, recordCount: Int)
