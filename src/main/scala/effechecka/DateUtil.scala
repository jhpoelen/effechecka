package effechecka

import java.util.Date

import org.joda.time.{DateTime, DateTimeZone}

trait DateUtil {

  def parseDate(dateString: String): Date = {
    new DateTime(dateString, DateTimeZone.UTC).toDate
  }

}
