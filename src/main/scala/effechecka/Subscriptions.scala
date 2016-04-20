package effechecka

import java.net.URL

import scala.collection.JavaConversions._
import com.datastax.driver.core.{Row, ResultSet, Session}
import com.typesafe.config.Config

trait Subscriptions {
  def subscribersOf(selector: OccurrenceSelector): List[URL]

  def subscribe(subscriber: URL, selector: OccurrenceSelector): URL

  def unsubscribe(subscriber: URL, selector: OccurrenceSelector): URL
}


trait SubscriptionsCassandra extends Subscriptions with Fetcher {

  implicit def session: Session

  implicit def config: Config

  def subscribersOf(selector: OccurrenceSelector): List[URL] = {
    val results: ResultSet = session.execute("SELECT subscriber FROM effechecka.subscriptions " + selectorWhereClause,
      selectorParams(selector): _*)
    val items: List[Row] = results.iterator.toList
    items.map(item => new URL(item.getString("subscriber")))
  }

  def subscriberParams(subscriber: URL, selector: OccurrenceSelector): List[String] = {
    selectorParams(selector) ::: List(subscriber.toString)
  }

  val selectorWhereClause: String = s"WHERE taxonSelector = ? AND wktString = ? AND traitSelector = ?"

  def subscribe(subscriber: URL, selector: OccurrenceSelector): URL = {
    session.execute(s"INSERT INTO effechecka.subscriptions (taxonSelector, wktString, traitSelector, subscriber) " +
      s"VALUES (?,?,?,?) ", subscriberParams(subscriber, selector): _*)
    subscriber
  }

  def unsubscribe(subscriber: URL, selector: OccurrenceSelector) = {
    session.execute(s"DELETE FROM effechecka.subscriptions " +
      selectorWhereClause + " AND subscriber = ?",
      subscriberParams(subscriber, selector): _*)
    subscriber
  }
}
