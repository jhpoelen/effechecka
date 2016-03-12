package effechecka

import com.typesafe.config.ConfigFactory
import com.datastax.driver.core._

trait Configure {
  def config = ConfigFactory.load()
  
  lazy val session: Session = {
    val cluster = Cluster.builder()
      .addContactPoint(config.getString("effechecka.cassandra.host")).build()
    val clusterSession = cluster.connect()
    clusterSession.execute("CREATE KEYSPACE IF NOT EXISTS effechecka WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")

    clusterSession.execute("CREATE TABLE IF NOT EXISTS effechecka.checklist (taxonselector TEXT, wktstring TEXT, traitselector TEXT, taxon TEXT, recordcount int, PRIMARY KEY((taxonselector, wktstring, traitselector), recordcount, taxon))")
    clusterSession.execute("CREATE TABLE IF NOT EXISTS effechecka.checklist_registry (taxonselector TEXT, wktstring TEXT, traitselector TEXT, status TEXT, recordcount int, PRIMARY KEY(taxonselector, wktstring, traitselector))")

    clusterSession.execute("CREATE TABLE IF NOT EXISTS effechecka.occurrence_collection (taxonselector TEXT, wktstring TEXT, traitselector TEXT, taxon TEXT, lat DOUBLE, lng DOUBLE, event_date TIMESTAMP, record_url TEXT, first_added_date TIMESTAMP, archive_url TEXT" +
      ", PRIMARY KEY((taxonselector, wktstring, traitselector), first_added_date, archive_url, record_url, taxon, event_date, lat, lng))")
    clusterSession.execute("CREATE TABLE IF NOT EXISTS effechecka.occurrence_collection_registry (taxonselector TEXT, wktstring TEXT, traitselector TEXT, status TEXT, recordcount int, PRIMARY KEY(taxonselector, wktstring, traitselector))")
    cluster.connect("effechecka")
  }

}
