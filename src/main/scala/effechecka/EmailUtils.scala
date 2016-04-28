package effechecka

import java.net.{URI, URLEncoder, URL}

import akka.http.scaladsl.model.headers.{Authorization, BasicHttpCredentials}
import akka.http.scaladsl.model.{FormData, HttpMethods, HttpRequest}
import HttpMethods._

case class Email(to: String, subject: String, text: String)

object EmailUtils {
  def mailgunRequestFor(email: Email, apiKey: String): HttpRequest = {
    HttpRequest(method = HttpMethods.POST,
      headers = List(Authorization(BasicHttpCredentials("api", apiKey))),
      uri = s"https://api.mailgun.net/v3/effechecka.org/messages",
      entity = FormData(Map[String, String]("to" -> email.to.toString,
        "from" -> "FreshData Notification (noreply) <noreply@effechecka.org>",
        "text" -> email.text,
        "subject" -> email.subject)).toEntity)
  }

  def urlFor(selector: OccurrenceSelector): URL = {
    new URL("http://gimmefreshdata.github.io/?" + queryParamsFor(selector))
  }

  def encode(str: String) = {
    URLEncoder.encode(str, "UTF-8")
  }


  def queryParamsFor(selector: OccurrenceSelector): String = {
    val queryParams = s"taxonSelector=${encode(selector.taxonSelector)}&wktString=${encode(selector.wktString)}&traitSelector=${encode(selector.traitSelector)}"
    queryParams
  }

  def unsubscribeUrlFor(event: SubscriptionEvent): URL = {
    new URL(s"http://c18node15.acis.ufl.edu/unsubscribe?subscriber=${encode(event.subscriber.toString)}&" + queryParamsFor(event.selector))
  }

  def unsubscribeTextFor(event: SubscriptionEvent): String = {
    s"If you no longer wish to receive these email, please visit ${unsubscribeUrlFor(event)} ."
  }

  def emailFor(event: SubscriptionEvent): Email = {
    val to = event.subscriber.getPath
    event.action match {
      case "subscribe" => {
        Email(to = to,
          subject = "[freshdata] subscribed to freshdata search",
          text = s"Hi!\nYou subscribed to the freshdata search available at ${urlFor(event.selector)}. \n${unsubscribeTextFor(event)}")
      }
      case "unsubscribe" => {
        Email(to = to,
          subject = "[freshdata] unsubscribed from freshdata search",
          text = s"Hi!\nYou are not longer subscribed to the freshdata search available at ${urlFor(event.selector)}.")
      }
      case "notify" => {
        Email(to = to,
          subject = "[freshdata] new data is available for your freshdata search",
          text = s"Hi!\nThe freshdata search that you subscribed to has new data, please see ${urlFor(event.selector)} for more details. \n${unsubscribeTextFor(event)}")
      }
    }
  }

}
