package effechecka

import org.effechecka.selector.OccurrenceSelector

case class ChecklistRequest(selector: OccurrenceSelector, limit: Option[Int] = None)
case class ChecklistItem(taxon: String, recordcount: Int)
case class Checklist(selector: OccurrenceSelector, status: String, items: List[ChecklistItem])
