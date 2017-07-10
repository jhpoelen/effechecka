package effechecka

import org.effechecka.selector.OccurrenceSelector

object OccurrenceSelectorUtil {

  def addUUIDIfNeeded(selector: OccurrenceSelector): OccurrenceSelector = {
    selector.uuid match {
      case Some(_) => selector
      case _ => selector.withUUID
    }
  }

}
