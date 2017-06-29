package effechecka

import java.util.UUID

import com.datastax.driver.core.ResultSet
import effechecka.selector.{OccurrenceSelector, UuidUtils}
import org.scalatest.{Matchers, WordSpec}

class SelectorRegistrySpec extends WordSpec with Matchers with SelectorRegistryCassandra with Configure {

  "cassandra selector registry" should {
    "register and find selector" in {
      truncate
      val selector: OccurrenceSelector = OccurrenceSelector("Insecta|Mammalia", "ENVELOPE(-150,-50,40,10)", "bodyMass greaterThan 2.7 kg")
      val selectorUuid: UUID = UuidUtils.uuidFor(selector)
      selectorFor(selectorUuid) should be(None)
      registerSelector(selector)
      selectorFor(selectorUuid) should be(Some(selector.withUUID))
    }

    "register and find selector with ttl and unregister" in {
      truncate
      val selector: OccurrenceSelector = OccurrenceSelector("Insecta|Mammalia", "ENVELOPE(-150,-50,40,10)", "bodyMass greaterThan 2.7 kg")
      val selectorUuid: UUID = UuidUtils.uuidFor(selector)
      selectorFor(selectorUuid) should be(None)
      registerSelector(selector, ttlSeconds = Some(10))
      selectorFor(selectorUuid) should be(Some(selector.withUUID))
      val noneUnregistered = unregisterSelectors((selector: OccurrenceSelector) => false)
      noneUnregistered should be(empty)
      selectorFor(selectorUuid) should be(Some(selector.withUUID))
      val unregistered = unregisterSelectors((selector: OccurrenceSelector) => true)
      unregistered should be(List(selector))
      selectorFor(selectorUuid) should be(None)
    }
  }

  def truncate: ResultSet = {
    session.execute("TRUNCATE effechecka.selector")
  }

}
