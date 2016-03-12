package effechecka

case class OccurrenceCollectionRequest(taxonSelector: String, wktString: String, traitSelector: String, limit: Int)
case class Occurrence(taxon: String, lat: Double, lng: Double, eventDate: Long, recordUrl: String, firstAddedDate: Long, archiveUrl: String)
case class OccurrenceCollection(taxonSelector: String, wktString: String, traitSelector: String, status: String, items: List[Occurrence])
