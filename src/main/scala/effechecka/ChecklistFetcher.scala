package effechecka

import com.datastax.driver.core.{Row, ResultSet}
import scala.collection.JavaConversions._

trait ChecklistFetcher extends Configure {
  def fetchChecklistItems(execute: (String, Seq[Any]) => ResultSet, taxonSelector: String, wktString: String): List[Map[String, Any]] = {
    val results: ResultSet = execute(checklistSelect, Seq(normalizeTaxonSelector(taxonSelector), wktString))
    val items: List[Row] = results.all.toList
    items.map(item => Map("taxon" -> item.getString("taxon"), "recordcount" -> item.getInt("recordcount")))
  }

  def fetchChecklistStatus(execute: (String, Any*) => ResultSet, taxonSelector: String, wktString: String): Option[String] = {
    val results: ResultSet = execute(checklistStatusSelect, Seq(normalizeTaxonSelector(taxonSelector), wktString))
    val items: List[Row] = results.all.toList
    items.map(_.getString("status")).headOption
  }

  def checklistSelect: String = {
    "SELECT taxon,recordcount FROM idigbio.checklist WHERE taxonselector = ? AND wktstring = ? ORDER BY recordcount DESC LIMIT 20"
  }

  def checklistStatusSelect: String = {
    "SELECT status FROM idigbio.checklist_registry WHERE taxonselector = ? AND wktstring = ? LIMIT 1"
  }

  def insertChecklistRequest(execute: (String, Any*) => ResultSet, taxonSelector: String, wktString: String): String = {
    val values = Seq(normalizeTaxonSelector(taxonSelector), wktString, "requested").map("'" + _ + "'").mkString(",")
    execute(s"INSERT INTO idigbio.checklist_registry (taxonselector, wktstring, status) VALUES ($values) using TTL 600", Seq())
    "requested"
  }

  def normalizeTaxonSelector(taxonSelector: String) = {
    taxonSelector.replace(',', '|')
  }

}

