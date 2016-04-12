package effechecka

case class OccurrenceCollectionRequest(taxonSelector: String, wktString: String, traitSelector: String, limit: Int)
case class Occurrence(taxon: String, lat: Double, lng: Double, start: Long, end: Long, id: String, added: Long, source: String)
case class OccurrenceCollection(taxonSelector: String, wktString: String, traitSelector: String, status: String, items: List[Occurrence])
case class OccurrenceMonitor(taxonSelector: String, wktString: String, traitSelector: String, status: String, recordCount: Int)
