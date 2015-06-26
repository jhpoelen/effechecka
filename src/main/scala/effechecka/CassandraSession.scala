package effechecka

import com.datastax.driver.core.{ResultSet, Cluster}

trait CassandraSession {
  def execute(query: String, params: Any*): ResultSet
}

trait LiveCassandraSession extends CassandraSession with Configure {
  def execute(query: String, params: Any*): ResultSet = {
    val cluster = Cluster.builder()
      .addContactPoint(config.getString("effechecka.cassandra.host")).build()
    val session = cluster.connect("idigbio")
    session.execute(query, params)
  }
}


