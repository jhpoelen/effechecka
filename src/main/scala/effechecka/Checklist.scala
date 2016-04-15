package effechecka

case class ChecklistRequest(selector: OccurrenceSelector, limit: Int)
case class ChecklistItem(taxon: String, recordcount: Int)
case class Checklist(selector: OccurrenceSelector, status: String, items: List[ChecklistItem])
