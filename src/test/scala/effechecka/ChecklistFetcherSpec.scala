package effechecka

import org.scalatest.{ Matchers, WordSpec, Ignore }

class ChecklistFetcherSpec extends WordSpec with Matchers with ChecklistFetcherCassandra with Configure {

  // needs running cassandra
  "Cassandra driver" should {

    "create a wellformed status query" in {
      def checklistTableCreate: String = {
        s"CREATE TABLE IF NOT EXISTS effechecka.checklist (taxonselector TEXT, wktstring TEXT, traitselector TEXT, taxon TEXT, recordcount int, PRIMARY KEY((taxonselector, wktstring, traitselector), recordcount, taxon))"
      }

      def checklistRegistryTableCreate: String = {
        s"CREATE TABLE IF NOT EXISTS effechecka.checklist_registry (taxonselector TEXT, wktstring TEXT, traitselector TEXT, status TEXT, recordcount int, PRIMARY KEY(taxonselector, wktstring, traitselector))"
      }

      def checklistKeySpaceCreate: String = {
        s"CREATE KEYSPACE IF NOT EXISTS effechecka WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }"
      }
      session.execute(checklistKeySpaceCreate)
      session.execute(checklistRegistryTableCreate)
      session.execute(checklistTableCreate)
      
      val request = ChecklistRequest("Insecta|Mammalia", "ENVELOPE(-150,-50,40,10)", "bodyMass greaterThan 2.7 kg", 2)
      insertRequest(request)
      session.execute("INSERT INTO effechecka.checklist (taxonselector, wktstring, traitSelector, taxon, recordcount) VALUES ('Insecta|Mammalia', 'ENVELOPE(-150,-50,40,10)', 'bodyMass greaterThan 2.7 kg', 'Aves|Donald duckus', 12)")
      val checklist = itemsFor(request)
      checklist.foreach(println)
      checklist should contain(ChecklistItem("Aves|Donald duckus", 12))
    }
  }

}
