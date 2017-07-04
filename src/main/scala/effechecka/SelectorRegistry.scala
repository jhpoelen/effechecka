package effechecka

import java.util.UUID

import org.effechecka.selector._

trait SelectorRegistry {
  def registerSelector(selector: OccurrenceSelector, ttlSeconds: Option[Int] = None): UUID

  def unregisterSelectors(filter: (OccurrenceSelector) => Boolean): List[OccurrenceSelector]

  def selectorFor(uuid: UUID): Option[OccurrenceSelector]
}



