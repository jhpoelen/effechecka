package effechecka

import java.nio.file.Paths
import java.util.UUID

import com.typesafe.config.Config
import effechecka.selector.{DateTimeSelector, OccurrenceSelector, UuidUtils}
import io.eels.FilePattern
import org.apache.hadoop.fs.{FileSystem, Path}

trait HDFSUtil extends DateUtil {

  implicit def config: Config

  protected implicit val fs: FileSystem

  def pathForSelector(occurrenceSelector: OccurrenceSelector): String = {
    UuidUtils.pathForSelector(occurrenceSelector)
  }

  def absolutePathForSelector(occurrenceSelector: OccurrenceSelector): String = {
    val suffix: String = pathForSelector(occurrenceSelector)
    val pathString = baseDir + "/" + suffix
    val pathFull = Paths.get(pathString)
    new Path(pathFull.toAbsolutePath.toString).toUri.toString
  }

  def pathForUUID(uuid: UUID) = {
    UuidUtils.pathForUUID(uuid)
  }

  def includeAll(path: Path) = FilePattern(path + "/*")

  def patternFor(suffix: String, pattern: (Path => FilePattern) = includeAll): Option[FilePattern] = {
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

  def selectPathByDateRange(request: OccurrenceRequest, path: String) = {
    FilePattern(path + "/*")
      .withFilter(pathFilterWithDateRange(request.added))
  }

  def pathFilterWithDateRange(added: DateTimeSelector): (Path) => Boolean = {
    val datePathPattern = ".*/y=([0-9]{4})/m=([0-9]{2})/d=([0-9]{2}).*".r
    x => {
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
