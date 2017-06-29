package effechecka

import com.datastax.driver.core._

import scala.collection.JavaConversions
import scala.collection.JavaConversions._
import com.typesafe.config.Config
import org.effechecka.selector.{DateTimeSelector, OccurrenceSelector}

trait Fetcher {
  def normalizeSelector(taxonSelector: String) = {
    taxonSelector.replace(',', '|')
  }

  def selectorParams(selector: OccurrenceSelector): List[String] = {
    List(normalizeSelector(selector.taxonSelector),
      selector.wktString,
      normalizeSelector(selector.traitSelector))
  }

  val selectorWhereClause: String = s"WHERE taxonSelector = ? AND wktString = ? AND traitSelector = ?"

}



trait OccurrenceCollectionFetcherCassandra extends OccurrenceCollectionFetcher
  with Fetcher
  with SparkSubmitter
  with DateUtil {
  implicit def session: Session

  implicit def config: Config


  def occurrencesFor(ocRequest: OccurrenceRequest): Iterator[Occurrence] = {
    val added = ocRequest.added
    val afterClause = added.after match {
      case Some(addedAfter) => Some(s"added > ?", parseDate(addedAfter))
      case _ => None
    }
    val beforeClause = added.before match {
      case Some(addedBefore) => Some(s"added < ?", parseDate(addedBefore))
      case _ => None
    }

    val additionalClauses = List(beforeClause, afterClause).flatten
    val query: String = (occurrenceCollectionSelect :: additionalClauses.map(_._1)).mkString(" AND ")

    val params = selectorParams(ocRequest.selector) ::: additionalClauses.map(_._2)

    val queryWithLimit = ocRequest.limit match {
      case Some(limit) => query + s" LIMIT $limit"
      case _ => query
    }

    val results: ResultSet = session.execute(queryWithLimit, params: _*)

    JavaConversions.asScalaIterator(results.iterator())
      .map(item => Occurrence(item.getString("taxon"), item.getDouble("lat"), item.getDouble("lng"), item.getDate("start").getTime, item.getDate("end").getTime, item.getString("id"), item.getDate("added").getTime, item.getString("source")))
  }

  def monitoredOccurrencesFor(source: String, added: DateTimeSelector = DateTimeSelector(), occLimit: Option[Int] = None): Iterator[String] = {
    val afterClause = added.after match {
      case Some(addedAfter) => Some(s"added > ?", parseDate(addedAfter))
      case _ => None
    }
    val beforeClause = added.before match {
      case Some(addedBefore) => Some(s"added < ?", parseDate(addedBefore))
      case _ => None
    }

    val additionalClauses = List(beforeClause, afterClause).flatten
    val monitoredOccurrencesSelect = s"SELECT id FROM effechecka.occurrence_first_added_search WHERE source = ? "
    val query: String = (monitoredOccurrencesSelect :: additionalClauses.map(_._1)).mkString(" AND ")

    val params = List(source) ::: additionalClauses.map(_._2)

    val queryWithLimit = occLimit match {
      case Some(limit) => query + s" LIMIT $limit"
      case _ => query
    }

    val results: ResultSet = session.execute(queryWithLimit, params: _*)

    JavaConversions.asScalaIterator(results.iterator())
      .map(item => item.getString("id"))
  }


  def monitors(): List[OccurrenceMonitor] = {
    val results: ResultSet = session.execute(occurrenceCollectionRegistrySelect)
    val items: List[Row] = results.iterator().toList
    items.map(item => {
      asOccurrenceMonitor(item)
    })
  }

  def asOccurrenceMonitor(item: Row): OccurrenceMonitor = {
    val selector = OccurrenceSelector(item.getString("taxonselector"), item.getString("wktstring"), item.getString("traitselector"))
    OccurrenceMonitor(selector.withUUID, Option(item.getString("status")), Option(item.getInt("recordcount")))
  }

  def monitorOf(selector: OccurrenceSelector): Option[OccurrenceMonitor] = {
    val queryMonitor: String = List(occurrenceCollectionRegistrySelect, selectorWhereClause, """LIMIT 1""").mkString(" ")
    val results: ResultSet = session.execute(queryMonitor, selectorParams(selector): _*)
    results.headOption match {
      case Some(item) => {
        val selector = OccurrenceSelector(item.getString("taxonselector"), item.getString("wktstring"), item.getString("traitselector"))
        Some(OccurrenceMonitor(selector.withUUID, Option(item.getString("status")), Option(item.getInt("recordcount"))))
      }
      case None => None
    }
  }

  def monitorsFor(source: String, id: String): Iterator[OccurrenceSelector] = {
    val results: ResultSet = session.execute(s"SELECT taxonselector, wktstring, traitselector " +
      s"FROM effechecka.occurrence_search " +
      s"WHERE source = ? AND id = ?", List(source, id): _*)

    if (results.iterator().hasNext) {
      JavaConversions.asScalaIterator(results.iterator())
        .map(item => OccurrenceSelector(item.getString("taxonselector"), item.getString("wktstring"), item.getString("traitselector")).withUUID)
    } else {
      Iterator[OccurrenceSelector]()
    }
  }


  def request(selector: OccurrenceSelector): String = {
    submitOccurrenceCollectionRequest(selector)
    insertRequest(selector)
  }

  def requestAll(): String = {
    submitOccurrenceCollectionsRefreshRequest()
"all requested"
  }

  def statusOf(selector: OccurrenceSelector): Option[String] = {
    val results: ResultSet = session.execute(occurrenceCollectionStatus, selectorParams(selector): _*)

    val items: List[Row] = results.iterator.toList
    items.map(_.getString("status")).headOption
  }

  def occurrenceCollectionSelect: String = {
    s"SELECT taxon, lat, lng, start, end, id, added, source FROM effechecka.occurrence_collection " + selectorWhereClause
  }

  def occurrenceCollectionRegistrySelect: String = {
    """SELECT taxonselector, wktstring, traitselector, status, recordcount FROM effechecka.occurrence_collection_registry"""
  }

  def occurrenceCollectionStatus: String = {
    "SELECT status FROM effechecka.occurrence_collection_registry " + selectorWhereClause + " LIMIT 1"
  }

  def insertRequest(selector: OccurrenceSelector, status: String = "requested"): String = {
    val values = (selectorParams(selector) ::: List(status)).map("'" + _ + "'").mkString(",")
    session.execute(s"INSERT INTO effechecka.occurrence_collection_registry (taxonselector, wktstring, traitSelector, status) VALUES ($values) using TTL 3600")
    "requested"
  }

}

