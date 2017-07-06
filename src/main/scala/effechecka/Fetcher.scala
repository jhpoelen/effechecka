package effechecka

import org.effechecka.selector.OccurrenceSelector

trait Fetcher {
  def normalizeSelector(taxonSelector: String) = {
    taxonSelector.replace(',', '|')
  }

  def selectorParams(selector: OccurrenceSelector): List[String] = {
    List(normalizeSelector(selector.taxonSelector),
      selector.wktString,
      normalizeSelector(selector.traitSelector))
  }

  val selectorWhereClause: String = s"WHERE taxonSelector = ? AND wktString = ? AND traitSelector = ?"

}
