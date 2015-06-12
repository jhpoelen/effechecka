package effechecka

import com.datastax.driver.core.{Session, Row, ResultSet, Cluster}
import scala.collection.JavaConversions._

trait ChecklistFetcher {
  def fetchChecklist(taxonSelector: String, wktString: String): List[Map[String, Any]] = {
    val results: ResultSet = session.execute(checklistSelect
      , taxonSelector.replace(',','|'), wktString)
    val items: List[Row] = results.all.toList
    items.map(item => Map("taxon" -> item.getString("taxon"), "recordcount" -> item.getInt("recordcount")))
  }

  def session: Session = {
    val cluster = Cluster.builder()
      .addContactPoint("localhost").build()
    cluster.connect("idigbio")
  }

  def checklistSelect: String = {
    "SELECT taxon,recordcount FROM idigbio.checklist WHERE taxonselector = ? AND wktstring = ? ORDER BY recordcount DESC LIMIT 20"
  }

}

