package effechecka

import com.typesafe.config.ConfigFactory
import com.datastax.driver.core._

trait Configure {
  def config = ConfigFactory.load()
  
  def session: Session = {
    val cluster = Cluster.builder()
      .addContactPoint(config.getString("effechecka.cassandra.host")).build()
    cluster.connect("effechecka")
  }

}
