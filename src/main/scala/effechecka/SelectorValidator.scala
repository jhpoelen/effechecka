package effechecka

import org.locationtech.spatial4j.context.jts.JtsSpatialContext
import org.locationtech.spatial4j.io.WKTReader

import scala.util.Try

trait SelectorValidator {

  def valid(selector: SelectorParams): Boolean = {
      Seq(validTaxonList _, validWktString _).forall(_(selector))
    }

    def invalid(selector: SelectorParams): Boolean = {
      !valid(selector)
    }

    def validWktString(occurrence: SelectorParams): Boolean = {
      Try {
        new WKTReader(JtsSpatialContext.GEO, null).parse(occurrence.wktString)
      }.isSuccess
    }

    def validTaxonList(occurrence: SelectorParams): Boolean = {
      occurrence.taxonSelector.matches("""[a-zA-Z,|\s]*""")
    }

}
