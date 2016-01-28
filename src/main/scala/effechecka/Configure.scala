package effechecka

import com.typesafe.config.ConfigFactory
import com.datastax.driver.core._

trait Configure {
  def config = ConfigFactory.load()
  
  def session: Session = {
    val cluster = Cluster.builder()
      .addContactPoint(config.getString("effechecka.cassandra.host")).build()
    val clusterSession = cluster.connect()
    clusterSession.execute("CREATE KEYSPACE IF NOT EXISTS effechecka WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    clusterSession.execute("CREATE TABLE IF NOT EXISTS effechecka.checklist (taxonselector TEXT, wktstring TEXT, traitselector TEXT, taxon TEXT, recordcount int, PRIMARY KEY((taxonselector, wktstring, traitselector), recordcount, taxon))")
    clusterSession.execute("CREATE TABLE IF NOT EXISTS effechecka.checklist_registry (taxonselector TEXT, wktstring TEXT, traitselector TEXT, status TEXT, recordcount int, PRIMARY KEY(taxonselector, wktstring, traitselector))")
    cluster.connect("effechecka")
  }

}
