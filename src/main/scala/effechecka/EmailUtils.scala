package effechecka

import java.net.{URL, URLEncoder}

object EmailUtils {

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

}
