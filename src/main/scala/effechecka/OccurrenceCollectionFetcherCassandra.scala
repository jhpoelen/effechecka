package effechecka

import com.datastax.driver.core._
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

  def occurrencesFor(ocRequest: OccurrenceCollectionRequest): List[Occurrence] = {
    val results: ResultSet = session.execute(occurrenceCollectionSelect(ocRequest.limit), normalizeTaxonSelector(ocRequest.selector.taxonSelector), ocRequest.selector.wktString, normalizeTaxonSelector(ocRequest.selector.traitSelector))
    val items: List[Row] = results.all.toList
    items.map(item => Occurrence(item.getString("taxon"), item.getDouble("lat"), item.getDouble("lng"), item.getDate("start").getTime, item.getDate("end").getTime, item.getString("id"), item.getDate("added").getTime, item.getString("source")))
  }

  def monitors(): List[OccurrenceMonitor] = {
    val results: ResultSet = session.execute(occurrenceCollectionRegistrySelect())
    val items: List[Row] = results.all.toList
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
    val results: ResultSet = session.execute(occurrenceCollectionStatus,
      normalizeTaxonSelector(ocRequest.selector.taxonSelector),
      ocRequest.selector.wktString,
      normalizeTaxonSelector(ocRequest.selector.traitSelector))

    val items: List[Row] = results.all.toList
    items.map(_.getString("status")).headOption
  }

  def occurrenceCollectionSelect(limit: Int): String = {
    s"SELECT taxon, lat, lng, start, end, id, added, source FROM effechecka.occurrence_collection WHERE taxonselector = ? AND wktstring = ? AND traitselector = ? LIMIT $limit"
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

