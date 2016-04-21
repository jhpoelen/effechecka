package effechecka

import java.net.URL

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import scala.util.{Success, Failure}
import scala.collection.JavaConversions._
import com.datastax.driver.core.{Row, ResultSet, Session}
import com.typesafe.config.Config

import scala.concurrent.{ExecutionContextExecutor, Future}

trait NotificationSender {
  def sendNotification(subscriber: URL, request: OccurrenceCollectionRequest)
}


trait NotificationSenderSendGrid extends NotificationSender with Fetcher {
  implicit val system: ActorSystem
  implicit val materializer: ActorMaterializer
  implicit val ec: ExecutionContextExecutor

  def sendNotification(subscriber: URL, request: OccurrenceCollectionRequest) = {
    subscriber.getProtocol match {
      case "mailto" => println(s"should send an email to [${subscriber.getPath}]")
      case _ => println("nothing to do")
    }
  }

}
