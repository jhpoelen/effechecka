package org.effechecka.selector

import java.util.UUID

import com.fasterxml.uuid.Generators
import org.scalatest.{Matchers, WordSpecLike}


class UuidUtilsSpec extends WordSpecLike with Matchers {

  "reconstruct effechecka namespace guid" in {
    // from http://www.ietf.org/rfc/rfc4122.txt
    val NameSpace_DNS = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
    val namespace = UUID.fromString(NameSpace_DNS)
    val generator = Generators.nameBasedGenerator(namespace)
    val effecheckaActual = generator.generate("effechecka.org")
    effecheckaActual should be(UUID.fromString("40d05094-803d-5bfa-b1e1-48975d2035fc"))
  }

  "effechecka selector guid" in {
    val selector = OccurrenceSelector("Animalia|Insecta", "ENVELOPE(-150,-50,40,10)", "")
    val expectedUUID: String = "55e4b0a0-bcd9-566f-99bc-357439011d85"

    UuidUtils.uuidFor(selector) should be(UUID.fromString(expectedUUID))

    val otherSelector = OccurrenceSelector("some other taxa", "some wkt", "some trait")
    UuidUtils.uuidFor(otherSelector) should not be UUID.fromString(expectedUUID)
  }

  "effechecka occurrence guid" in {
    val occurrenceUUID = UuidUtils.generator.generate("http://www.inaturalist.org/observations/1053719")
    occurrenceUUID should be(UUID.fromString("baa722b5-7b86-5d11-82f8-e516c86156c9"))
  }

}