package effechecka

import java.util.UUID

import com.datastax.driver.core._
import com.typesafe.config.Config

trait SelectorRegistry {
  def registerSelector(selector: OccurrenceSelector): UUID

  def selectorFor(uuid: UUID): Option[OccurrenceSelector]
}

trait SelectorRegistryCassandra extends SelectorRegistry with Fetcher {
  implicit def session: Session

  implicit def config: Config

  def selectorFor(uuid: UUID): Option[OccurrenceSelector] = {
    val results: ResultSet = session.execute(s"SELECT taxonselector, wktstring, traitselector FROM effechecka.selector WHERE uuid = ? LIMIT 1", uuid)
    if (results.isExhausted) {
      None
    } else {
      val row = results.one()
      Some(OccurrenceSelector(taxonSelector = row.getString("taxonselector"), wktString = row.getString("wktstring"), traitSelector = row.getString("traitselector")).withUUID)
    }
  }

  def registerSelector(selector: OccurrenceSelector): UUID = {
    val ttlSeconds: Long = config.getLong("effechecka.monitor.ttlSeconds")
    val selectorUuid: UUID = UuidUtils.uuidFor(selector)
    session.execute(s"INSERT INTO effechecka.selector (uuid, taxonselector, wktstring, traitSelector) VALUES (?,?,?,?)",
      selectorUuid, selector.taxonSelector, selector.wktString, selector.traitSelector)
    session.execute(s"INSERT INTO effechecka.monitors (taxonselector, wktstring, traitSelector, accessed_at) VALUES (?,?,?,dateOf(NOW())) USING TTL $ttlSeconds",
      selector.taxonSelector, selector.wktString, selector.traitSelector)
    selectorUuid
  }

}

