package effechecka

import java.util.UUID

import com.datastax.driver.core.ResultSet
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

    "register and find selector with ttl" in {
      truncate
      val selector: OccurrenceSelector = OccurrenceSelector("Insecta|Mammalia", "ENVELOPE(-150,-50,40,10)", "bodyMass greaterThan 2.7 kg")
      val selectorUuid: UUID = UuidUtils.uuidFor(selector)
      selectorFor(selectorUuid) should be(None)
      registerSelector(selector, ttlSeconds = Some(10))
      selectorFor(selectorUuid) should be(Some(selector.withUUID))
    }
  }

  def truncate: ResultSet = {
    session.execute("TRUNCATE effechecka.selector")
  }

}
