package effechecka

import java.net.{URI, URLEncoder, URL}

import akka.http.scaladsl.model.headers.{Authorization, BasicHttpCredentials}
import akka.http.scaladsl.model.{FormData, HttpMethods, HttpRequest}
import HttpMethods._

case class Email(to: String, subject: String, text: String)

object EmailUtils {

  var URL_DEFAULT =  "http://gimmefreshdata.github.io/"

  def mailgunRequestFor(email: Email, apiKey: String): HttpRequest = {
    HttpRequest(method = HttpMethods.POST,
      headers = List(Authorization(BasicHttpCredentials("api", apiKey))),
      uri = s"https://api.mailgun.net/v3/effechecka.org/messages",
      entity = FormData(Map[String, String]("to" -> email.to.toString,
        "from" -> "FreshData Notification (noreply) <noreply@effechecka.org>",
        "text" -> email.text,
        "subject" -> email.subject)).toEntity)
  }

  def urlFor(selector: OccurrenceSelector, baseURL: String = URL_DEFAULT): URL = {
    urlWithQuery(baseURL = baseURL, query = queryParamsFor(selector))
  }

  def urlWithQuery(baseURL: String = URL_DEFAULT, query: String): URL = {
    new URL(s"$baseURL?$query")
  }

  def urlFor(event: SubscriptionEvent): URL = {
    val after = event.addedAfter match {
      case Some(addedAfter) => s"&addedAfter=$addedAfter"
      case _ => ""
    }

    val before = event.addedBefore match {
      case Some(addedBefore) => s"&addedBefore=$addedBefore"
      case _ => ""
    }

    urlWithQuery(query = s"${queryParamsFor(event.selector)}$after$before")
  }

  def encode(str: String) = {
    URLEncoder.encode(str, "UTF-8")
  }


  def queryParamsFor(selector: OccurrenceSelector): String = {
    s"taxonSelector=${encode(selector.taxonSelector)}&wktString=${encode(selector.wktString)}&traitSelector=${encode(selector.traitSelector)}"
  }

  def unsubscribeUrlFor(event: SubscriptionEvent): URL = {
    new URL(s"http://apihack-c18.idigbio.org/unsubscribe?subscriber=${encode(event.subscriber.toString)}&" + queryParamsFor(event.selector))
  }

  def unsubscribeTextFor(event: SubscriptionEvent): String = {
    s"If you no longer wish to receive these email, please visit ${unsubscribeUrlFor(event)} ."
  }

  def emailHeader = {
    "Hi!\n\n"
  }

  def emailFooter = {
    """
      |
      |Thanks!
      |
      |PS Fresh Data is an early stage prototype. Please share your feedback at https://github.com/gimmefreshdata/freshdata/issues/new .
      |
    """.stripMargin
  }

  def emailFor(event: SubscriptionEvent): Email = {
    val to = event.subscriber.getPath
    event.action match {
      case "subscribe" => {
        Email(to = to,
          subject = "[freshdata] subscribed to freshdata search",
          text = s"${emailHeader}You subscribed to the freshdata search available at ${urlFor(event.selector)}. \n\n${unsubscribeTextFor(event)} $emailFooter")
      }
      case "unsubscribe" => {
        Email(to = to,
          subject = "[freshdata] unsubscribed from freshdata search",
          text = s"${emailHeader}You are not longer subscribed to the freshdata search available at ${urlFor(event.selector)}. $emailFooter")
      }
      case "notify" => {
        Email(to = to,
          subject = "[freshdata] new data is available for your freshdata search",
          text = s"${emailHeader}New data is available for a freshdata search you subscribed to. Please see ${urlFor(event)} for more details. \n\n${unsubscribeTextFor(event)} $emailFooter")
      }
    }
  }

}
