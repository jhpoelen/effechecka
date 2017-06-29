package effechecka

import java.util.UUID

import com.datastax.driver.core._
import com.typesafe.config.Config
import org.effechecka.selector.{OccurrenceSelector, UuidUtils}

import scala.collection.JavaConverters._

trait SelectorRegistry {
  def registerSelector(selector: OccurrenceSelector, ttlSeconds: Option[Int] = None): UUID

  def unregisterSelectors(filter: (OccurrenceSelector) => Boolean): List[OccurrenceSelector]

  def selectorFor(uuid: UUID): Option[OccurrenceSelector]
}

trait SelectorRegistryCassandra extends SelectorRegistry with Fetcher {
  implicit def session: Session

  implicit def config: Config

  def rowToSelector(row: Row): OccurrenceSelector = {
    OccurrenceSelector(
      taxonSelector = row.getString("taxonselector"),
      wktString = row.getString("wktstring"),
      traitSelector = row.getString("traitselector")
    )
  }

  def selectorFor(uuid: UUID): Option[OccurrenceSelector] = {
    val results: ResultSet = session.execute(s"SELECT taxonselector, wktstring, traitselector FROM effechecka.selector WHERE uuid = ? LIMIT 1", uuid)
    if (results.isExhausted) {
      None
    } else {
      val row = results.one()
      Some(rowToSelector(row).withUUID)
    }
  }

  def registerSelector(selector: OccurrenceSelector, ttlSeconds: Option[Int] = None): UUID = {
    val ttlSecondsValue: Int = ttlSeconds.getOrElse(config.getInt("effechecka.monitor.ttlSeconds"))
    val selectorUuid: UUID = UuidUtils.uuidFor(selector)
    session.execute(s"INSERT INTO effechecka.selector (uuid, taxonselector, wktstring, traitSelector) VALUES (?,?,?,?)",
      selectorUuid, selector.taxonSelector, selector.wktString, selector.traitSelector)
    session.execute(s"INSERT INTO effechecka.monitors (taxonselector, wktstring, traitSelector, accessed_at) VALUES (?,?,?,dateOf(NOW())) USING TTL $ttlSecondsValue",
      selector.taxonSelector, selector.wktString, selector.traitSelector)
    selectorUuid
  }

  def unregisterSelectors(filter: (OccurrenceSelector) => Boolean): List[OccurrenceSelector] = {
    val rows: ResultSet = session.execute(s"SELECT taxonselector, wktstring, traitselector FROM effechecka.selector")
    rows.iterator.asScala
      .map(rowToSelector)
      .filter(filter)
      .map { selector =>
        val selectorUuid: UUID = UuidUtils.uuidFor(selector)
        session.execute(s"DELETE FROM effechecka.selector where uuid = ?", selectorUuid)
        session.execute(s"DELETE FROM effechecka.monitors where taxonselector = ? AND wktstring = ? AND traitSelector = ?", selector.taxonSelector, selector.wktString, selector.traitSelector)
        selector
      }.toList
  }

}

