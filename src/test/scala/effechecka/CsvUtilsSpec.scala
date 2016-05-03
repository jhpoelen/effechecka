package effechecka


import org.scalatest._

class CsvUtilsSpec extends WordSpec with Matchers {

  "url for selector" in {
    val row: String = CsvUtils.toOccurrenceRow(Occurrence(taxon = "a taxon", start = 123L, end = 12345L, lat= 12.2, lng = 11.1, added = 333L, source = "a source", id="some id"))
    row.trim should be(""""a taxon","a taxon",12.2,11.1,1970-01-01T00:00:00.123Z,"some id",1970-01-01T00:00:00.333Z,"a source"""")
    row should endWith("\n")
  }

}