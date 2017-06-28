package effechecka

import java.nio.file.Paths
import java.util.UUID

import com.typesafe.config.Config
import io.eels.FilePattern
import org.apache.hadoop.fs.{FileSystem, Path}

trait HDFSUtil extends DateUtil {

  implicit def config: Config

  protected implicit val fs: FileSystem

  def pathForSelector(occurrenceSelector: OccurrenceSelector): String = {
    val suffix: String = pathForUUID(UuidUtils.uuidFor(occurrenceSelector))
    val prefix = "occurrencesForMonitor"
    s"$prefix/$suffix"
  }

  def absolutePathForSelector(occurrenceSelector: OccurrenceSelector): String = {
    val suffix: String = pathForSelector(occurrenceSelector)
    val pathString = baseDir + "/" + suffix
    val pathFull = Paths.get(pathString)
    new Path(pathFull.toAbsolutePath.toString).toUri.toString
  }

  def pathForUUID(uuid: UUID) = {
    val f0 = uuid.toString.substring(0, 2)
    val f1 = uuid.toString.substring(2, 4)
    val f2 = uuid.toString.substring(4, 6)
    val suffix = s"$f0/$f1/$f2/$uuid/"
    suffix
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
      .withFilter(pathFilterWithDateRange(request))
  }


  def pathFilterWithDateRange(request: OccurrenceRequest): (Path) => Boolean = {
    val datePathPattern = ".*/y=([0-9]{4})/m=([0-9]{2})/d=([0-9]{2})".r
    x => {
      println("filtering " + x)
      x.toUri.toString match {
        case datePathPattern(year, month, day) => {
          val pathDate = parseDate(s"$year-$month-$day")
          val upper = request.added.before match {
            case Some(end) => Seq(com.google.common.collect.Range.lessThan(parseDate(end)))
            case None => Seq()
          }
          val lower = request.added.after match {
            case Some(start) => Seq(com.google.common.collect.Range.greaterThan(parseDate(start)))
            case None => Seq()
          }
          !(upper ++ lower).exists(!_.contains(pathDate))
        }
        case _ => false
      }
    }

  }
}
