package effechecka

trait OccurrenceCollectionFetcherHDFS extends OccurrenceCollectionFetcher {

  def occurrencesFor(ocRequest: OccurrenceRequest): Iterator[Occurrence] = {
    Iterator(Occurrence(taxon = "a|taxon", lat = 12.1d, lng = 11.1d, start = 0L, end = 1L, id = "some:id", added = 2L, source = "a source"))
  }

  def monitoredOccurrencesFor(source: String, added: DateTimeSelector = DateTimeSelector(), occLimit: Option[Int] = None): Iterator[String] = {
    Iterator("some:id")
  }


  val monitor = OccurrenceMonitor(selector = OccurrenceSelector("Animalia|Insecta", "ENVELOPE(-150,-50,40,10)", ""), status = None, recordCount = Some(123))

  def monitors(): List[OccurrenceMonitor] = {
    List(monitor)
  }

  def monitorOf(selector: OccurrenceSelector): Option[OccurrenceMonitor] = {
    Some(monitor)
  }

  def monitorsFor(source: String, id: String): Iterator[OccurrenceSelector] = {
    Iterator(monitor.selector)
  }


  def request(selector: OccurrenceSelector): String = {
    "unknown"
  }

  def requestAll(): String = {
    "all requested"
  }

  def statusOf(selector: OccurrenceSelector): Option[String] = {
    Some("unknown")
  }

}

