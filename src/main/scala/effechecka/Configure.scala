package effechecka

import com.typesafe.config.ConfigFactory

trait Configure {
  def config = ConfigFactory.load()

}
