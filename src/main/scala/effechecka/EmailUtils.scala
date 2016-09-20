package effechecka

import java.net.{URI, URLEncoder, URL}

import akka.http.scaladsl.model.headers.{Authorization, BasicHttpCredentials}
import akka.http.scaladsl.model.{FormData, HttpMethods, HttpRequest}
import HttpMethods._

case class Email(to: String, subject: String, text: String)

object EmailUtils {

  val BASE_URL_DEFAULT = "http://api.effechecka.org"
  val URL_DEFAULT = s"$BASE_URL_DEFAULT/view"

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

  def uuidQuery(selector: OccurrenceSelector): String = s"uuid=${UuidUtils.uuidFor(selector)}"

  def uuidUrlFor(event: SubscriptionEvent, baseURL: String = URL_DEFAULT): URL = {
    urlWithQuery(baseURL = baseURL, query = uuidQueryFor(event))
  }

  def urlWithQuery(baseURL: String = URL_DEFAULT, query: String): URL = {
    new URL(s"$baseURL?$query")
  }

  def urlFor(event: SubscriptionEvent): URL = {
    uuidUrlFor(event)
  }

  def uuidQueryFor(event: SubscriptionEvent): String = {
    val (after: String, before: String) = queryAfterBefore(event.request.added)
    s"${uuidQuery(event.request.selector)}$after$before"
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

  def unsubscribeUrlFor(event: SubscriptionEvent): URL = {
    new URL(s"$BASE_URL_DEFAULT/unsubscribe?subscriber=${encode(event.subscriber.toString)}&" + uuidQuery(event.request.selector))
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
          text = s"${emailHeader}You subscribed to the freshdata search available at ${uuidUrlFor(event)}. \n\n${unsubscribeTextFor(event)} $emailFooter")
      }
      case "unsubscribe" => {
        Email(to = to,
          subject = "[freshdata] unsubscribed from freshdata search",
          text = s"${emailHeader}You are not longer subscribed to the freshdata search available at ${urlFor(event)}. $emailFooter")
      }
      case "notify" => {
        Email(to = to,
          subject = "[freshdata] new data is available for your freshdata search",
          text = s"${emailHeader}New data is available for a freshdata search you subscribed to. Please see ${urlFor(event)} for more details. \n\n${unsubscribeTextFor(event)} $emailFooter")
      }
    }
  }

}
