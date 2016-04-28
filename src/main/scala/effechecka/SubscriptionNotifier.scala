package effechecka

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpRequest
import akka.stream._
import akka.stream.scaladsl._
import spray.json._


object SubscriptionNotifier extends App
  with Configure
  with NotificationFeedSourceKafka
  with OccurrenceCollectionFetcherCassandra
  with SubscriptionsCassandra
  with SubscriptionFeed {


  implicit val system = ActorSystem("effechecka")
  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  val logger = Logging(system, getClass)


  def whenDataAvailableScheduleNotificationForSubscribers = {
    import GraphDSL.Implicits._

    GraphDSL.create() { implicit builder =>

      val incomingSelectorEvents = builder.add(feed)
      val subscriptionEventFeed = builder.add(subscriptionHandler("effechecka-subsciption"))

      val generateSubscriptionEvents = builder.add(Flow[String]
        .map(jsonString => jsonString.parseJson.convertTo[MonitorStatus])
        .filter(_.status == "ready")
        .map(_.selector)
        .mapConcat(ocSelector => {
          val ocRequest = OccurrenceCollectionRequest(ocSelector, 1)
          val occurrences: List[Occurrence] = occurrencesFor(ocRequest)

          if (occurrences.nonEmpty) {
            subscribersOf(ocSelector).map(subscriber => SubscriptionEvent(ocSelector, subscriber, "notify"))
          } else {
            List()
          }
        })
      )

      incomingSelectorEvents ~> generateSubscriptionEvents ~> subscriptionEventFeed

      ClosedShape
    }
  }

  def subscriberEventToMailgunRequest(apikey: String = "someApiKey") = {
    import GraphDSL.Implicits._

    GraphDSL.create() { implicit builder =>
      val generateHttpRequest = builder.add(Flow[Email].map(email => {
        EmailUtils.mailgunRequestFor(email, apikey)
      }))

      val generateEmail = builder.add(Flow[SubscriptionEvent]
        .filter(event => List("subscribe", "unsubscribe", "notify").contains(event.action))
        .map(EmailUtils.emailFor))

      generateEmail ~> generateHttpRequest

      FlowShape(generateEmail.in, generateHttpRequest.out)
    }
  }

  def subscriberFeedToSubscriberEvent = {
    import GraphDSL.Implicits._

    GraphDSL.create() { implicit builder =>
      val subscriptionEvents = builder.add(subscriberFeed)
      val parseSubscriptionEvent = builder.add(Flow[String]
        .map(jsonString => jsonString.parseJson.convertTo[SubscriptionEvent]))

      subscriptionEvents ~> parseSubscriptionEvent
      SourceShape(parseSubscriptionEvent.out)
    }
  }

  def deliverNotificationsToSubscribers(apiKey: String = "someApiKey") = {
    import GraphDSL.Implicits._

        GraphDSL.create() { implicit builder =>
          val eventSource = builder.add(subscriberFeedToSubscriberEvent)
          val toMailgunRequests = builder.add(subscriberEventToMailgunRequest(apiKey))
          val sendRequest = builder.add(Sink.foreach[HttpRequest](
            Http().singleRequest(_)
          ))

          eventSource ~> toMailgunRequests ~> sendRequest
          ClosedShape
        }
  }

  materializer.materialize(deliverNotificationsToSubscribers(config.getString("effechecka.mailgun.apikey")))
  materializer.materialize(whenDataAvailableScheduleNotificationForSubscribers)

}
