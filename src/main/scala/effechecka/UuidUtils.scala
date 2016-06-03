package effechecka

import java.util.UUID
import com.fasterxml.uuid.{Generators, StringArgGenerator}

object UuidUtils {
  val effecheckaNamespace = UUID.fromString("40d05094-803d-5bfa-b1e1-48975d2035fc")

  lazy val generator: StringArgGenerator = Generators.nameBasedGenerator(effecheckaNamespace)

  def uuidFor(selector: OccurrenceSelector): UUID = {
    generator.generate(EmailUtils.queryParamsFor(selector))
  }

}
