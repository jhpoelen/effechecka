package effechecka

import java.nio.file.Paths
import java.util.UUID

import com.sksamuel.exts.Logging
import com.typesafe.config.Config
import io.eels.FilePattern
import org.apache.hadoop.fs.{FileSystem, Path}
import org.effechecka.selector.{DateTimeSelector, UuidUtils}

trait HDFSUtil extends DateUtil with Logging {

  implicit def config: Config

  protected implicit val fs: FileSystem

  def pathForSelector(selector: Selector): String = {
    UuidUtils.pathForUUID(UUID.fromString(selector.withUUID().uuid))
  }

  def absolutePathForSelector(selector: Selector): String = {
    val suffix: String = pathForSelector(selector)
    val pathString = baseDir + "/" + suffix
    val pathFull = Paths.get(pathString)
    new Path(pathFull.toAbsolutePath.toString).toUri.toString
  }

  def pathForUUID(uuid: UUID) = {
    UuidUtils.pathForUUID(uuid)
  }

  def includeAll(path: Path) = FilePattern(path + "/*")

  def patternFor(suffix: String, pattern: (Path => FilePattern) = includeAll): Option[FilePattern] = {
    val pathString = (baseDir + "/" + suffix).replaceFirst("hdfs://", "")
    logger.info(s"looking for [$pathString]")
    val path = new Path(pathString)
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

  def selectPathByDateRange(request: OccurrenceRequest, path: String) = {
    FilePattern(path + "/*")
      .withFilter(pathFilterWithDateRange(request.added))
  }

  def pathFilterWithDateRange(added: DateTimeSelector): (Path) => Boolean = {
    x => {
      if (added.after.isEmpty && added.before.isEmpty) {
        true
      } else {
        val datePathPattern = ".*/y=([0-9]{4})/m=([0-9]{2})/d=([0-9]{2}).*".r
        x.toUri.toString match {
          case datePathPattern(year, month, day) => {
            val pathDate = parseDate(s"$year-$month-$day")
            val upper = added.before match {
              case Some(end) => Some(com.google.common.collect.Range.lessThan(parseDate(end)))
              case None => None
            }
            val lower = added.after match {
              case Some(start) => Some(com.google.common.collect.Range.greaterThan(parseDate(start)))
              case None => None
            }
            !Seq(upper, lower).flatten.exists(!_.contains(pathDate))
          }
          case _ => false
        }
      }
    }
  }
}
