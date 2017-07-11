package effechecka

case class ChecklistRequest(selector: Selector, limit: Option[Int] = None)
case class ChecklistItem(taxon: String, recordcount: Int)
case class Checklist(selector: Selector, status: String, items: List[ChecklistItem])
