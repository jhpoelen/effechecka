package effechecka

import com.datastax.driver.core.{ResultSet, Row}
import org.specs2.mutable.Specification
import org.specs2.mock._
import scala.collection.JavaConversions._


trait TestCassandraSession extends CassandraSession with Mockito {
  def execute(query: String, params: Any*): ResultSet = {
    val mockResult = mock[ResultSet]
    val mockRow = mock[Row]
    mockRow.getString("taxon") returns "checklist item"
    mockRow.getInt("recordcount") returns 1

    mockResult.all returns List(mockRow).toList
  }
}


class ChecklistFetcherSpec extends Specification with ChecklistFetcher with TestCassandraSession {

  "Checklist fetcher" should {
    "create a populated checklist" in {
      val checklist = fetchChecklistItems(execute, "Insecta|Mammalia", "ENVELOPE(-150,-50,40,10)")
      checklist must contain(Map("taxon" -> "checklist item", "recordcount" -> 1))
    }

    "create a checklist" in {
      val checklist = fetchChecklistItems(execute, "Insecta|Mammalia", "ENVELOPE(-150,-50,40,10)")
      checklist must contain(Map("taxon" -> "checklist item", "recordcount" -> 1))
    }
  }

}
