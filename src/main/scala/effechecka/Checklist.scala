package effechecka

case class ChecklistRequest(taxonSelector: String, wktString: String, traitSelector: String, limit: Int)
case class ChecklistItem(taxon: String, recordcount: Int)
case class Checklist(taxonSelector: String, wktString: String, traitSelector: String, status: String, items: List[ChecklistItem])
