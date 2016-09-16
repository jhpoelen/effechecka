package effechecka

import org.locationtech.spatial4j.context.jts.JtsSpatialContext
import org.locationtech.spatial4j.io.WKTReader

import scala.util.Try

trait SelectorValidator {

  def valid(selector: OccurrenceSelector): Boolean = {
      Seq(validTaxonList _, validWktString _).forall(_(selector))
    }

    def invalid(selector: OccurrenceSelector): Boolean = {
      !valid(selector);
    }

    def validWktString(occurrence: OccurrenceSelector): Boolean = {
      Try {
        new WKTReader(JtsSpatialContext.GEO, null).parse(occurrence.wktString)
      }.isSuccess
    }

    def validTaxonList(occurrence: OccurrenceSelector): Boolean = {
      occurrence.taxonSelector.matches("""[a-zA-Z,|\s]+""")
    }

}
