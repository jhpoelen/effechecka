package effechecka

import java.nio.file.Paths
import java.util.UUID

import com.typesafe.config.Config
import io.eels.FilePattern
import org.apache.hadoop.fs.{FileSystem, Path}

trait HDFSUtil {

  implicit def config: Config

  protected implicit val fs: FileSystem

  def pathForSelector(occurrenceSelector: OccurrenceSelector): String = {
    val suffix: String = pathForUUID(UuidUtils.uuidFor(occurrenceSelector))
    val prefix = "occurrencesForMonitor"
    s"$prefix/$suffix"
  }

  def pathForUUID(uuid: UUID) = {
    val f0 = uuid.toString.substring(0, 2)
    val f1 = uuid.toString.substring(2, 4)
    val f2 = uuid.toString.substring(4, 6)
    val suffix = s"$f0/$f1/$f2/$uuid/"
    suffix
  }

  def includeAll(path: Path) = FilePattern(path + "/*")

  def patternFor(suffix: String, pattern: (Path => FilePattern) = includeAll) = {
    val pathString = baseDir + "/" + suffix
    val pathFull = Paths.get(pathString)
    val path = new Path(pathFull.toAbsolutePath.toString)
    if (fs.exists(path)) {
      val resourcePath = fs.resolvePath(path)
      Some(pattern(resourcePath))
    } else {
      None
    }
  }
  protected def baseDir = {
    config.getString("effechecka.monitor.dir")
  }


}
