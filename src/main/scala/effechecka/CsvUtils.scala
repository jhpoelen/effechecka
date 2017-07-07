package effechecka

import java.net.{URI, URLEncoder, URL}

import akka.http.scaladsl.model.headers.{Authorization, BasicHttpCredentials}
import akka.http.scaladsl.model.{FormData, HttpMethods, HttpRequest}
import HttpMethods._
import org.joda.time.format.ISODateTimeFormat

object CsvUtils {

  def toOccurrenceRow(occurrence: Occurrence): String = {
    val taxonString = if (occurrence.taxon == null) "" else occurrence.taxon
    val lastTaxon = taxonString.split('|').filter(_.nonEmpty).reverse.headOption match {
      case Some(taxon) => taxon.trim
      case _ => ""
    }

    val occurrenceUrl = urlForOccurrenceId(occurrence).getOrElse("")
    Seq(lastTaxon, taxonString,
      occurrence.lat, occurrence.lng,
      dateOrEmpty(occurrence.start),
      occurrence.id,
      dateOrEmpty(occurrence.added),
      occurrence.source,
      occurrenceUrl)
      .map(value => if (value == null) "" else value)
      .mkString("\n", "\t", "")
  }

  private def dateOrEmpty(timestamp: Long) = {
    val toISO: (Long) => String = ISODateTimeFormat.dateTime().withZoneUTC().print
    toISO(timestamp)
  }

  def urlForOccurrenceId(occurrence: Occurrence): Option[URI] = {
    occurrence.source match {
      case "gbif" => Some(new URI("http", null, "www.gbif.org", -1, "/occurrence/search", s"OCCURRENCE_ID=${occurrence.id}", null))
      case "idigbio" => Some(new URI("http", null, "portal.idigbio.org", -1, "/search", s"""rq={"occurrenceid":"${occurrence.id}"}""", null))
      case _ => None
    }
  }
}
