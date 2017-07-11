package effechecka

import org.effechecka.selector.{OccurrenceSelector, UuidUtils}

trait Selector {
  def withUUID(): SelectorUUID = {
    this match {
      case s: SelectorParams =>
        val occurrenceSelector = OccurrenceSelector(taxonSelector = s.taxonSelector, wktString = s.wktString, traitSelector = s.traitSelector)
        SelectorUUID(taxonSelector = Some(s.taxonSelector),
          wktString = Some(s.wktString),
          traitSelector = Some(s.traitSelector),
          uuid = UuidUtils.uuidFor(occurrenceSelector).toString)
      case s: SelectorUUID =>
        s
    }
  }
}


case class SelectorParams(taxonSelector: String = "",
                          wktString: String = "",
                          traitSelector: String = "") extends Selector {
}

case class SelectorUUID(uuid: String, taxonSelector: Option[String] = None,
                        wktString: Option[String] = None,
                        traitSelector: Option[String] = None) extends Selector {
}