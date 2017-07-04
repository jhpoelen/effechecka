package effechecka

import com.datastax.driver.core._

trait ConfigureCassandra extends Configure {

  lazy val session: Session = {
    val cluster = Cluster.builder()
      .addContactPoint(config.getString("effechecka.cassandra.host")).build()
    val clusterSession = cluster.connect()
    clusterSession.execute("CREATE KEYSPACE IF NOT EXISTS effechecka WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")

    val selectorColumns = "taxonselector TEXT, wktstring TEXT, traitselector TEXT"
    val selectors = "taxonselector, wktstring, traitselector"
    val selectorKey = s"($selectors)"

    clusterSession.execute(s"CREATE TABLE IF NOT EXISTS effechecka.checklist ($selectorColumns, taxon TEXT, recordcount int, PRIMARY KEY($selectorKey, recordcount, taxon))")
    clusterSession.execute(s"CREATE TABLE IF NOT EXISTS effechecka.checklist_registry ($selectorColumns, status TEXT, recordcount int, PRIMARY KEY($selectorKey))")

    clusterSession.execute(s"CREATE TABLE IF NOT EXISTS effechecka.occurrence_collection ($selectorColumns, taxon TEXT, lat DOUBLE, lng DOUBLE, start TIMESTAMP, end TIMESTAMP, id TEXT, added TIMESTAMP, source TEXT" +
      s", PRIMARY KEY($selectorKey, added, source, id, taxon, start, end, lat, lng))")
    clusterSession.execute(s"CREATE TABLE IF NOT EXISTS effechecka.occurrence_collection_registry ($selectorColumns, status TEXT, recordcount int, PRIMARY KEY($selectorKey))")

    clusterSession.execute(s"CREATE TABLE IF NOT EXISTS effechecka.occurrence_first_added_search (source TEXT, added TIMESTAMP, id TEXT" +
          s", PRIMARY KEY(source, added, id))")
    clusterSession.execute(s"CREATE TABLE IF NOT EXISTS effechecka.occurrence_search (source TEXT, id TEXT, $selectorColumns" +
          s", PRIMARY KEY((source), id, $selectors))")

    clusterSession.execute(s"CREATE TABLE IF NOT EXISTS effechecka.selector (uuid UUID, $selectorColumns" +
          s", PRIMARY KEY(uuid))")

    clusterSession.execute(s"CREATE TABLE IF NOT EXISTS effechecka.monitors ($selectorColumns, accessed_at TIMESTAMP" +
          s", PRIMARY KEY($selectorKey))")


    clusterSession.execute(s"CREATE TABLE IF NOT EXISTS effechecka.subscriptions ($selectorColumns, subscriber TEXT, PRIMARY KEY($selectorKey, subscriber))")

    cluster.connect("effechecka")
  }

}
