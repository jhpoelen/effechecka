package effechecka

import com.datastax.driver.core._
import scala.collection.JavaConversions._
import org.apache.spark.deploy.SparkSubmit
import com.typesafe.config.Config

trait ChecklistFetcher2 {
  def itemsFor(checklist: Checklist): List[ChecklistItem]
  def statusOf(checklist: Checklist): Option[String]
  def request(checklist: Checklist): String
}

trait ChecklistFetcher extends ChecklistFetcher2 {
  implicit def session: Session
  implicit def config: Config

  def fetchChecklistItems(taxonSelector: String, wktString: String, traitSelector: String, limit: Int = 20): List[Map[String, Any]] = {
    fetchChecklistItems2(Checklist(taxonSelector, wktString, traitSelector, limit))
  }

  def fetchChecklistItems2(checklist: Checklist): List[Map[String, Any]] = {
    val results: ResultSet = session.execute(checklistSelect(checklist.limit), normalizeTaxonSelector(checklist.taxonSelector), checklist.wktString, normalizeTaxonSelector(checklist.traitSelector))
    val items: List[Row] = results.all.toList
    items.map(item => Map("taxon" -> item.getString("taxon"), "recordcount" -> item.getInt("recordcount")))
  }

  def itemsFor(checklist: Checklist): List[ChecklistItem] = {
    val checklist_items = fetchChecklistItems2(checklist)
    checklist_items.map { item =>
      List("taxon", "recordcount") flatMap (item get _) match {
        case Some(taxon: String) :: Some(recordCount: Int) :: _ => Some(ChecklistItem(taxon, recordCount))
        case _ => None
      }
    }.flatten
  }
  
  def request(checklist: Checklist): String = {
    SparkSubmit.main(Array("--master", config.getString("effechecka.spark.master.url"), "--class", "ChecklistGenerator", "--deploy-mode", "cluster", "--executor-memory", "32G", config.getString("effechecka.spark.job.jar"), config.getString("effechecka.data.dir") + "*occurrence.txt", checklist.taxonSelector.replace(',', '|'), checklist.wktString, "cassandra", checklist.traitSelector.replace(',', '|'), config.getString("effechecka.data.dir") + "*traits.csv"))
    insertChecklistRequest(checklist.taxonSelector, checklist.wktString, checklist.traitSelector)                  
  }

  def fetchChecklistStatus(taxonSelector: String, wktString: String, traitSelector: String): Option[String] = {
    val results: ResultSet = session.execute(checklistStatusSelect, normalizeTaxonSelector(taxonSelector), wktString, normalizeTaxonSelector(traitSelector))
    val items: List[Row] = results.all.toList
    items.map(_.getString("status")).headOption
  }
  
   def statusOf(checklist: Checklist): Option[String] = {
     fetchChecklistStatus(checklist.taxonSelector, checklist.wktString, checklist.traitSelector)
   }

  def checklistSelect(limit: Int): String = {
    s"SELECT taxon,recordcount FROM effechecka.checklist WHERE taxonselector = ? AND wktstring = ? AND traitselector = ? ORDER BY recordcount DESC LIMIT $limit"
  }

  def checklistStatusSelect: String = {
    "SELECT status FROM effechecka.checklist_registry WHERE taxonselector = ? AND wktstring = ? AND traitselector = ? LIMIT 1"
  }

  def insertChecklistRequest(taxonSelector: String, wktString: String, traitSelector: String): String = {
    val values = Seq(normalizeTaxonSelector(taxonSelector), wktString, traitSelector, "requested").map("'" + _ + "'").mkString(",")
    session.execute(s"INSERT INTO effechecka.checklist_registry (taxonselector, wktstring, traitSelector, status) VALUES ($values) using TTL 600")
    "requested"
  }

  def normalizeTaxonSelector(taxonSelector: String) = {
    taxonSelector.replace(',', '|')
  }

}

