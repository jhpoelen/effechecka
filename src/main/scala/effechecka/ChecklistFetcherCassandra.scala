package effechecka

import com.datastax.driver.core._
import scala.collection.JavaConversions._
import org.apache.spark.deploy.SparkSubmit
import com.typesafe.config.Config

trait ChecklistFetcher {
  def itemsFor(checklist: ChecklistRequest): List[ChecklistItem]
  def statusOf(checklist: ChecklistRequest): Option[String]
  def request(checklist: ChecklistRequest): String
}

trait ChecklistFetcherCassandra extends ChecklistFetcher with Fetcher {
  implicit def session: Session
  implicit def config: Config

  def itemsFor(checklist: ChecklistRequest): List[ChecklistItem] = {
    val results: ResultSet = session.execute(checklistSelect(checklist.limit), normalizeTaxonSelector(checklist.taxonSelector), checklist.wktString, normalizeTaxonSelector(checklist.traitSelector))
    val items: List[Row] = results.all.toList
    items.map(item => ChecklistItem(item.getString("taxon"), item.getInt("recordcount")))
  }

  def request(checklist: ChecklistRequest): String = {
    SparkSubmit.main(Array("--master",
      config.getString("effechecka.spark.master.url"),
      "--class", "ChecklistGenerator",
      "--deploy-mode", "cluster",
      config.getString("effechecka.spark.job.jar"),
      "-f", "cassandra",
      "-c", "\"" + config.getString("effechecka.data.dir") + "gbif-idigbio.parquet" + "\"",
      "-t", "\"" + config.getString("effechecka.data.dir") + "traitbank/*.csv" + "\"",
      "\"" + checklist.taxonSelector.replace(',', '|') +"\"",
      "\"" + checklist.wktString + "\"",
      "\"" + checklist.traitSelector.replace(',', '|') + "\""))
    insertRequest(checklist)
  }

  def statusOf(checklist: ChecklistRequest): Option[String] = {
    val results: ResultSet = session.execute(checklistStatusSelect, normalizeTaxonSelector(checklist.taxonSelector), checklist.wktString, normalizeTaxonSelector(checklist.traitSelector))
    val items: List[Row] = results.all.toList
    items.map(_.getString("status")).headOption
  }

  def checklistSelect(limit: Int): String = {
    s"SELECT taxon,recordcount FROM effechecka.checklist WHERE taxonselector = ? AND wktstring = ? AND traitselector = ? ORDER BY recordcount DESC LIMIT $limit"
  }

  def checklistStatusSelect: String = {
    "SELECT status FROM effechecka.checklist_registry WHERE taxonselector = ? AND wktstring = ? AND traitselector = ? LIMIT 1"
  }

  def insertRequest(checklist: ChecklistRequest): String = {
    val values = Seq(normalizeTaxonSelector(checklist.taxonSelector), checklist.wktString, checklist.traitSelector, "requested").map("'" + _ + "'").mkString(",")
    session.execute(s"INSERT INTO effechecka.checklist_registry (taxonselector, wktstring, traitSelector, status) VALUES ($values) using TTL 600")
    "requested"
  }

}

