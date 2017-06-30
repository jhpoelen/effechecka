package effechecka

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import org.effechecka.selector.OccurrenceSelector
import org.scalatest.{Matchers, WordSpecLike}

class ChecklistFetcherCassandraSpec extends TestKit(ActorSystem("SparkIntegrationTest"))
  with WordSpecLike with Matchers with ChecklistFetcherCassandra with Configure {

  implicit val materializer = ActorMaterializer()(system)
  implicit val ec = system.dispatcher

  // needs running cassandra
  "Cassandra driver" should {
    "create a wellformed status query" in {
      val request = ChecklistRequest(OccurrenceSelector("Insecta|Mammalia", "ENVELOPE(-150,-50,40,10)", "bodyMass greaterThan 2.7 kg"), Some(2))
      insertRequest(request)
      session.execute("INSERT INTO effechecka.checklist (taxonselector, wktstring, traitSelector, taxon, recordcount) VALUES ('Insecta|Mammalia', 'ENVELOPE(-150,-50,40,10)', 'bodyMass greaterThan 2.7 kg', 'Aves|Donald duckus', 12)")
      val checklist = itemsFor(request)
      checklist.toList should contain(ChecklistItem("Aves|Donald duckus", 12))
    }
  }

}
