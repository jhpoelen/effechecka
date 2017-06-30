package effechecka

import java.nio.file.Paths

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

trait HDFSTestUtil {

  private val resourcePath = Paths.get(getClass.getResource("/hdfs-layout/base.txt").toURI).getParent.toAbsolutePath
  val config: Config = ConfigFactory.parseString(s"""effechecka.monitor.dir = "$resourcePath"""")


  implicit val configHadoop: Configuration = new Configuration()
  implicit val fs: FileSystem = FileSystem.get(configHadoop)

}
