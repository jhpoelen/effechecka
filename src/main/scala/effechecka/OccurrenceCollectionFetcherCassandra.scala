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
}

trait OccurrenceCollectionFetcher {
  def occurrencesFor(request: OccurrenceCollectionRequest): List[Occurrence]

  def statusOf(request: OccurrenceCollectionRequest): Option[String]

  def request(request: OccurrenceCollectionRequest): String

  def monitors(): List[OccurrenceMonitor]
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

    val params = selectorParams(ocRequest) ::: additionalClauses.map(_._2)

    val queryWithLimit: String = query + s" LIMIT ${ocRequest.limit}"

    val results: ResultSet = session.execute(queryWithLimit, params: _*)

    val items: List[Row] = results.iterator().toList
    items.map(item => Occurrence(item.getString("taxon"), item.getDouble("lat"), item.getDouble("lng"), item.getDate("start").getTime, item.getDate("end").getTime, item.getString("id"), item.getDate("added").getTime, item.getString("source")))
  }

  def selectorParams(ocRequest: OccurrenceCollectionRequest): List[String] = {
    List(normalizeTaxonSelector(ocRequest.selector.taxonSelector),
      ocRequest.selector.wktString,
      normalizeTaxonSelector(ocRequest.selector.traitSelector))
  }

  def monitors(): List[OccurrenceMonitor] = {
    val results: ResultSet = session.execute(occurrenceCollectionRegistrySelect())
    val items: List[Row] = results.iterator().toList
    items.map(item => {
      val selector = OccurrenceSelector(item.getString("taxonselector"), item.getString("wktstring"), item.getString("traitselector"))
      OccurrenceMonitor(selector, item.getString("status"), item.getInt("recordcount"))
    })
  }

  def request(ocRequest: OccurrenceCollectionRequest): String = {
    SparkSubmit.main(Array("--master",
      config.getString("effechecka.spark.master.url"),
      "--class", "OccurrenceCollectionGenerator",
      "--deploy-mode", "cluster",
      config.getString("effechecka.spark.job.jar"),
      "-f", "cassandra",
      "-c", "\"" + config.getString("effechecka.data.dir") + "gbif-idigbio.parquet" + "\"",
      "-t", "\"" + config.getString("effechecka.data.dir") + "traitbank/*.csv" + "\"",
      "\"" + ocRequest.selector.taxonSelector.replace(',', '|') + "\"",
      "\"" + ocRequest.selector.wktString + "\"",
      "\"" + ocRequest.selector.traitSelector.replace(',', '|') + "\""))
    insertRequest(ocRequest)
  }

  def statusOf(ocRequest: OccurrenceCollectionRequest): Option[String] = {
    val results: ResultSet = session.execute(occurrenceCollectionStatus, selectorParams(ocRequest): _*)

    val items: List[Row] = results.iterator.toList
    items.map(_.getString("status")).headOption
  }

  def occurrenceCollectionSelect: String = {
    s"SELECT taxon, lat, lng, start, end, id, added, source FROM effechecka.occurrence_collection WHERE taxonselector = ? AND wktstring = ? AND traitselector = ?"
  }

  def occurrenceCollectionRegistrySelect(): String = {
    s"SELECT taxonselector, wktstring, traitselector, status, recordcount FROM effechecka.occurrence_collection_registry"
  }

  def occurrenceCollectionStatus: String = {
    "SELECT status FROM effechecka.occurrence_collection_registry WHERE taxonselector = ? AND wktstring = ? AND traitselector = ? LIMIT 1"
  }

  def insertRequest(request: OccurrenceCollectionRequest): String = {
    val values = Seq(normalizeTaxonSelector(request.selector.taxonSelector), request.selector.wktString, request.selector.traitSelector, "requested").map("'" + _ + "'").mkString(",")
    session.execute(s"INSERT INTO effechecka.occurrence_collection_registry (taxonselector, wktstring, traitSelector, status) VALUES ($values) using TTL 600")
    "requested"
  }

}

