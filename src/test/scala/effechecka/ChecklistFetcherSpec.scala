package effechecka

import org.scalatest.{Matchers, WordSpec, Ignore}

class ChecklistFetcherSpec extends WordSpec with Matchers with ChecklistFetcherCassandra with Configure {

  // needs running cassandra
  "Cassandra driver" should {
    "create a wellformed status query" in {
      val request = ChecklistRequest("Insecta|Mammalia", "ENVELOPE(-150,-50,40,10)", "bodyMass greaterThan 2.7 kg", 2)
      insertRequest(request)
      session.execute("INSERT INTO effechecka.checklist (taxonselector, wktstring, traitSelector, taxon, recordcount) VALUES ('Insecta|Mammalia', 'ENVELOPE(-150,-50,40,10)', 'bodyMass greaterThan 2.7 kg', 'Aves|Donald duckus', 12)")
      val checklist = itemsFor(request)
      checklist.foreach(println)
      checklist should contain(ChecklistItem("Aves|Donald duckus", 12))
    }
  }

}
