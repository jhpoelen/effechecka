package effechecka


trait ChecklistFetcherHDFS extends ChecklistFetcher {

  def itemsFor(checklist: ChecklistRequest): Iterator[ChecklistItem] = {
    Iterator(ChecklistItem("a|name", 1234))
  }

  def request(checklist: ChecklistRequest): String = {
    "unknown"
  }

  def statusOf(checklist: ChecklistRequest): Option[String] = {
    Some("unknown")
  }

  def pathFor(occurrenceSelector: OccurrenceSelector) = {
    val uuid = UuidUtils.uuidFor(occurrenceSelector)
    val f0 = uuid.toString.substring(0, 2)
    val f1 = uuid.toString.substring(2, 4)
    val f2 = uuid.toString.substring(4, 6)
    s"occurrencesForMonitor/$f0/$f1/$f2/$uuid/checklist/"
  }



}

