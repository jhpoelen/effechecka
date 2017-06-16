package effechecka

trait ChecklistFetcher {
  def itemsFor(checklist: ChecklistRequest): List[ChecklistItem]
  def statusOf(checklist: ChecklistRequest): Option[String]
  def request(checklist: ChecklistRequest): String
}
