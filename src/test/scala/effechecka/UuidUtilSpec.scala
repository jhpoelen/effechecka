package effechecka

import java.util.UUID

import com.fasterxml.uuid.{Generators, StringArgGenerator}

import org.scalatest._



class UuidUtilSpec extends WordSpecLike with Matchers {


  "reconstruct effechecka namespace guid" in {
    // from http://www.ietf.org/rfc/rfc4122.txt
    val NameSpace_DNS = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
    val namespace = UUID.fromString(NameSpace_DNS)
    val generator = Generators.nameBasedGenerator(namespace)
    val effecheckaActual = generator.generate("effechecka.org")
    effecheckaActual should be(UUID.fromString("40d05094-803d-5bfa-b1e1-48975d2035fc"))
  }

  "effechecka selector guid" in {
    val selector = OccurrenceSelector("some taxa", "some wkt", "some trait")

    val expectedUUID: String = "c7483fed-ff5c-54b1-a436-37884e585f11"

    UuidUtils.uuidFor(selector) should be(UUID.fromString(expectedUUID))

    val otherSelector = OccurrenceSelector("some other taxa", "some wkt", "some trait")
    UuidUtils.uuidFor(otherSelector) should not be(UUID.fromString(expectedUUID))
  }


}