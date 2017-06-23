package effechecka

import com.datastax.driver.core._
import scala.collection.JavaConversions._
import com.typesafe.config.Config



trait ChecklistFetcherCassandra extends ChecklistFetcher
  with Fetcher
  with SparkSubmitter {
  implicit def session: Session
  implicit def config: Config

  def itemsFor(checklist: ChecklistRequest): Iterator[ChecklistItem] = {
    val results: ResultSet = session.execute(checklistSelect(checklist.limit),
      selectorParams(checklist.selector): _*)
    val items: Iterator[Row] = results.iterator
    items.map(item => ChecklistItem(item.getString("taxon"), item.getInt("recordcount")))
  }

  def request(checklist: ChecklistRequest): String = {
    submitChecklistRequest(checklist)
    insertRequest(checklist)
  }

  def statusOf(checklist: ChecklistRequest): Option[String] = {
    val results: ResultSet = session.execute(checklistStatusSelect, selectorParams(checklist.selector): _*)
    val items: List[Row] = results.iterator.toList
    items.map(_.getString("status")).headOption
  }

  def checklistSelect(limit: Int): String = {
    s"SELECT taxon,recordcount FROM effechecka.checklist WHERE taxonselector = ? AND wktstring = ? AND traitselector = ? ORDER BY recordcount DESC LIMIT $limit"
  }

  def checklistStatusSelect: String = {
    "SELECT status FROM effechecka.checklist_registry WHERE taxonselector = ? AND wktstring = ? AND traitselector = ? LIMIT 1"
  }

  def insertRequest(checklist: ChecklistRequest): String = {
    val values = (selectorParams(checklist.selector) ::: List("requested")).map("'" + _ + "'").mkString(",")
    val requestQuietTime = config.getInt("effechecka.request.quietTimeSeconds")
    session.execute(s"INSERT INTO effechecka.checklist_registry (taxonselector, wktstring, traitSelector, status) VALUES ($values) using TTL $requestQuietTime")
    "requested"
  }

}

