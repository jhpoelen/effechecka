package effechecka


import org.scalatest._

class CsvUtilsSpec extends WordSpec with Matchers {

  "line for occurrence" in {
    val row: String = CsvUtils.toOccurrenceRow(Occurrence(taxon = "a taxon", start = 123L, end = 12345L, lat= 12.2, lng = 11.1, added = 333L, source = "a source", id="some id"))
    row.trim should be(""""a taxon","a taxon",12.2,11.1,1970-01-01T00:00:00.123Z,"some id",1970-01-01T00:00:00.333Z,"a source",""""")
    row should endWith("\n")
  }

  "url for id unknown source" in {
    val unknownSource = CsvUtils.urlForOccurrenceId(Occurrence(taxon = "a taxon", start = 123L, end = 12345L, lat = 12.2, lng = 11.1, added = 333L, source = "a source", id = "some id"))
    unknownSource should be(None)
  }

  "url for id from idigbio" in {
    val unknownSource = CsvUtils.urlForOccurrenceId(Occurrence(taxon = "a taxon", start = 123L, end = 12345L, lat = 12.2, lng = 11.1, added = 333L, source = "idigbio", id = "10030"))
    unknownSource.map(_.toString) should be(Some("http://portal.idigbio.org/search?rq=%7B%22occurrenceid%22:%2210030%22%7B"))
  }

  "url for id from gbif" in {
    val unknownSource = CsvUtils.urlForOccurrenceId(Occurrence(taxon = "a taxon", start = 123L, end = 12345L, lat = 12.2, lng = 11.1, added = 333L, source = "gbif", id = "10030"))
    unknownSource.map(_.toString) should be(Some("http://www.gbif.org/occurrence/search?OCCURRENCE_ID=10030"))
  }

  "url for id from gbif escaping" in {
    val unknownSource = CsvUtils.urlForOccurrenceId(Occurrence(taxon = "a taxon", start = 123L, end = 12345L, lat = 12.2, lng = 11.1, added = 333L, source = "gbif", id = "some,, "))
    unknownSource.map(_.toString) should be(Some("http://www.gbif.org/occurrence/search?OCCURRENCE_ID=some,,%20"))
  }

}