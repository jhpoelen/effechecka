package effechecka

import java.util.UUID

import org.effechecka.selector._

trait SelectorRegistryNOP extends SelectorRegistry {
  def registerSelector(selector: OccurrenceSelector, ttlSeconds: Option[Int] = None): UUID = {
    UuidUtils.uuidFor(selector)
  }

  def unregisterSelectors(filter: (OccurrenceSelector) => Boolean): List[OccurrenceSelector] = {
    List()
  }

  def selectorFor(uuid: UUID): Option[OccurrenceSelector] = {
    None
  }
}



