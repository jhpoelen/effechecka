package effechecka

case class Checklist(taxonSelector: String, wktString: String, traitSelector: String, limit: Int)
case class ChecklistItem(name: String, recordCount: Int)
case class Checklist2(taxonSelector: String, wktString: String, traitSelector: String, status: String, items: List[ChecklistItem])
