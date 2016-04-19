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

    val selectorColumns = "taxonselector TEXT, wktstring TEXT, traitselector TEXT"
    val selectorKey = "(taxonselector, wktstring, traitselector)"

    clusterSession.execute(s"CREATE TABLE IF NOT EXISTS effechecka.checklist ($selectorColumns, taxon TEXT, recordcount int, PRIMARY KEY($selectorKey, recordcount, taxon))")
    clusterSession.execute(s"CREATE TABLE IF NOT EXISTS effechecka.checklist_registry ($selectorColumns, status TEXT, recordcount int, PRIMARY KEY($selectorKey))")

    clusterSession.execute(s"CREATE TABLE IF NOT EXISTS effechecka.occurrence_collection ($selectorColumns, taxon TEXT, lat DOUBLE, lng DOUBLE, start TIMESTAMP, end TIMESTAMP, id TEXT, added TIMESTAMP, source TEXT" +
      s", PRIMARY KEY($selectorKey, added, source, id, taxon, start, end, lat, lng))")
    clusterSession.execute(s"CREATE TABLE IF NOT EXISTS effechecka.occurrence_collection_registry ($selectorColumns, status TEXT, recordcount int, PRIMARY KEY($selectorKey))")

    clusterSession.execute(s"CREATE TABLE IF NOT EXISTS effechecka.subscriptions ($selectorColumns, subscriber TEXT, PRIMARY KEY($selectorKey, subscriber))")

    cluster.connect("effechecka")
  }

}
