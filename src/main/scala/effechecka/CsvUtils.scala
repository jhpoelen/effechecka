package effechecka

import java.net.{URI, URLEncoder, URL}

import akka.http.scaladsl.model.headers.{Authorization, BasicHttpCredentials}
import akka.http.scaladsl.model.{FormData, HttpMethods, HttpRequest}
import HttpMethods._
import org.joda.time.format.ISODateTimeFormat

object CsvUtils {

  def toOccurrenceRow(occurrence: Occurrence): String = {
    val lastTaxon = occurrence.taxon.split('|').reverse.head.trim
    val toISO: (Long) => String = ISODateTimeFormat.dateTime().withZoneUTC().print
    val dateString = toISO(occurrence.start)

    val occurrenceUrl = urlForOccurrenceId(occurrence).getOrElse("")
    s""""$lastTaxon","${occurrence.taxon}",${occurrence.lat},${occurrence.lng},$dateString,"${occurrence.id}",${toISO(occurrence.added)},"${occurrence.source}","$occurrenceUrl"\n"""
  }

  def urlForOccurrenceId(occurrence: Occurrence): Option[URI] = {
    occurrence.source match {
      case "gbif" => Some(new URI("http", null, "www.gbif.org", -1, "/occurrence/search", s"OCCURRENCE_ID=${occurrence.id}", null))
      case "idigbio" => Some(new URI("http", null, "portal.idigbio.org", -1, "/search", s"""rq={"occurrenceid":"${occurrence.id}"}""", null))
      case _ => None
    }
  }
}
