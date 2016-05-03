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
    s""""$lastTaxon","${occurrence.taxon}",${occurrence.lat},${occurrence.lng},$dateString,"${occurrence.id}",${toISO(occurrence.added)},"${occurrence.source}"\n"""
  }

}
