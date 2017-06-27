package effechecka

import java.nio.file.Paths

import com.typesafe.config.{Config, ConfigFactory}

trait HDFSTestUtil {

  private val resourcePath = Paths.get(getClass.getResource("/hdfs-layout/base.txt").toURI).getParent.toAbsolutePath
  val config: Config = ConfigFactory.parseString(s"""effechecka.monitor.dir = "$resourcePath"""")


}
