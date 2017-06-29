package effechecka

import effechecka.selector.OccurrenceSelector
import org.scalatest._


class SelectorValidatorSpec extends WordSpecLike with Matchers with SelectorValidator {
  val someValidSelector: OccurrenceSelector = OccurrenceSelector(taxonSelector = "Ariopsis felis", wktString = "ENVELOPE(-150,-50,40,10)", traitSelector = "")


  "invalid taxon selector" in {
    invalid(someValidSelector.copy(taxonSelector = "9|9|9")) should be(true)
  }

  "valid taxon selector" in {
    valid(someValidSelector) should be(true)
  }

  "valid empty taxon selector" in {
    valid(someValidSelector.copy(taxonSelector = "")) should be(true)
  }


}