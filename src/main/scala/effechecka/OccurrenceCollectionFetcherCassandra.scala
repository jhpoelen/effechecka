package effechecka

import com.datastax.driver.core._
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import scala.collection.JavaConversions._
import org.apache.spark.deploy.SparkSubmit
import com.typesafe.config.Config

trait Fetcher {
  def normalizeTaxonSelector(taxonSelector: String) = {
    taxonSelector.replace(',', '|')
  }

  def selectorParams(selector: OccurrenceSelector): List[String] = {
    List(normalizeTaxonSelector(selector.taxonSelector),
      selector.wktString,
      normalizeTaxonSelector(selector.traitSelector))
  }

  val selectorWhereClause: String = s"WHERE taxonSelector = ? AND wktString = ? AND traitSelector = ?"

}

trait OccurrenceCollectionFetcher {
  def occurrencesFor(request: OccurrenceCollectionRequest): List[Occurrence]

  def statusOf(selector: OccurrenceSelector): Option[String]

  def request(selector: OccurrenceSelector): String

  def monitors(): List[OccurrenceMonitor]

  def monitorOf(selector: OccurrenceSelector): Option[OccurrenceMonitor]
}

trait OccurrenceCollectionFetcherCassandra extends OccurrenceCollectionFetcher with Fetcher {
  implicit def session: Session

  implicit def config: Config

  def parseDate(dateString: String) = {
    new DateTime(dateString, DateTimeZone.UTC).toDate
  }

  def occurrencesFor(ocRequest: OccurrenceCollectionRequest): List[Occurrence] = {
    val afterClause = ocRequest.addedAfter match {
      case Some(addedAfter) => Some(s"added > ?", parseDate(addedAfter))
      case _ => None
    }
    val beforeClause = ocRequest.addedBefore match {
      case Some(addedBefore) => Some(s"added < ?", parseDate(addedBefore))
      case _ => None
    }

    val additionalClauses = List(beforeClause, afterClause).flatten
    val query: String = (occurrenceCollectionSelect :: additionalClauses.map(_._1)).mkString(" AND ")

    val params = selectorParams(ocRequest.selector) ::: additionalClauses.map(_._2)

    val queryWithLimit: String = query + s" LIMIT ${ocRequest.limit}"

    val results: ResultSet = session.execute(queryWithLimit, params: _*)

    val items: List[Row] = results.iterator().toList
    items.map(item => Occurrence(item.getString("taxon"), item.getDouble("lat"), item.getDouble("lng"), item.getDate("start").getTime, item.getDate("end").getTime, item.getString("id"), item.getDate("added").getTime, item.getString("source")))
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
    OccurrenceMonitor(selector, Option(item.getString("status")), Option(item.getInt("recordcount")))
  }

  def monitorOf(selector: OccurrenceSelector): Option[OccurrenceMonitor] = {
    val queryMonitor: String = List(occurrenceCollectionRegistrySelect, selectorWhereClause, """LIMIT 1""").mkString(" ")
    val results: ResultSet = session.execute(queryMonitor, selectorParams(selector): _*)
    results.headOption match {
      case Some(item) => {
        val selector = OccurrenceSelector(item.getString("taxonselector"), item.getString("wktstring"), item.getString("traitselector"))
        Some(OccurrenceMonitor(selector, Option(item.getString("status")), Option(item.getInt("recordcount"))))
      }
      case None => None
    }
  }

  def request(selector: OccurrenceSelector): String = {
    SparkSubmit.main(Array("--master",
      config.getString("effechecka.spark.master.url"),
      "--class", "OccurrenceCollectionGenerator",
      "--deploy-mode", "cluster",
      "--executor-memory", "2g",
      config.getString("effechecka.spark.job.jar"),
      "-f", "cassandra",
      "-c", "\"" + config.getString("effechecka.data.dir") + "gbif-idigbio.parquet" + "\"",
      "-t", "\"" + config.getString("effechecka.data.dir") + "traitbank/*.csv" + "\"",
      "\"" + selector.taxonSelector.replace(',', '|') + "\"",
      "\"" + selector.wktString + "\"",
      "\"" + selector.traitSelector.replace(',', '|') + "\""))
    insertRequest(selector)
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

