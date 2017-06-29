package org.effechecka.selector

import java.net.{URL, URLEncoder}
import java.util.UUID

import com.fasterxml.uuid.{Generators, StringArgGenerator}

object UuidUtils {
  val effecheckaNamespace = UUID.fromString("40d05094-803d-5bfa-b1e1-48975d2035fc")

  lazy val generator: StringArgGenerator = Generators.nameBasedGenerator(effecheckaNamespace)

  def uuidFor(selector: OccurrenceSelector): UUID = {
    generator.generate(queryParamsFor(selector))
  }

  val BASE_URL_DEFAULT = "http://api.effechecka.org"
  val URL_DEFAULT = s"$BASE_URL_DEFAULT/view"

  def urlFor(selector: OccurrenceSelector, baseURL: String = URL_DEFAULT): URL = {
    urlWithQuery(baseURL = baseURL, query = queryParamsFor(selector))
  }

  def uuidQuery(selector: OccurrenceSelector): String = s"uuid=${UuidUtils.uuidFor(selector)}"

  def urlWithQuery(baseURL: String = URL_DEFAULT, query: String): URL = {
    new URL(s"$baseURL?$query")
  }

  def queryAfterBefore(added: DateTimeSelector): (String, String) = {
    val after = added.after match {
      case Some(addedAfter) => s"&addedAfter=${encode(addedAfter)}"
      case _ => ""
    }

    val before = added.before match {
      case Some(addedBefore) => s"&addedBefore=${encode(addedBefore)}"
      case _ => ""
    }
    (after, before)
  }

  def encode(str: String) = {
    URLEncoder.encode(str, "UTF-8")
  }


  def queryParamsFor(selector: OccurrenceSelector, added: DateTimeSelector = DateTimeSelector()): String = {
    val (after, before) = queryAfterBefore(added)
    val taxonSelector: String = encode(selector.taxonSelector)
    val wktString: String = encode(selector.wktString)
    val traitSelector: String = encode(selector.traitSelector)
    s"taxonSelector=$taxonSelector&wktString=$wktString&traitSelector=$traitSelector$after$before"
  }

  def pathForSelector(occurrenceSelector: OccurrenceSelector): String = {
    val suffix: String = pathForUUID(UuidUtils.uuidFor(occurrenceSelector))
    val prefix = "occurrencesForMonitor"
    s"$prefix/$suffix"
  }

  def pathForUUID(uuid: UUID) = {
    val f0 = uuid.toString.substring(0, 2)
    val f1 = uuid.toString.substring(2, 4)
    val f2 = uuid.toString.substring(4, 6)
    s"$f0/$f1/$f2/$uuid"
  }



}
