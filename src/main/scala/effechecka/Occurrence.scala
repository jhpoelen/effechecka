package effechecka

case class OccurrenceSelector(taxonSelector: String, wktString: String, traitSelector: String, uuid: Option[String] = None) {
  def withUUID = this.copy(uuid = Some(UuidUtils.uuidFor(this).toString))
}
case class DateTimeSelector(before: Option[String] = None, after: Option[String] = None)

case class OccurrenceCollectionRequest(selector: OccurrenceSelector, limit: Option[Int], added: DateTimeSelector = DateTimeSelector())
case class Occurrence(taxon: String, lat: Double, lng: Double, start: Long, end: Long, id: String, added: Long, source: String)
case class OccurrenceCollection(selector: OccurrenceSelector, status: Option[String], items: List[Occurrence] = List())
case class OccurrenceMonitor(selector: OccurrenceSelector, status: Option[String], recordCount: Option[Int])
