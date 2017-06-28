package effechecka

import java.util.{Date, UUID}

import akka.actor.ActorSystem
import akka.http.scaladsl.model.DateTime
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import io.eels.FilePattern
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest._


class HDFSUtilSpec extends TestKit(ActorSystem("IntegrationTest"))
  with WordSpecLike with Matchers with HDFSTestUtil with HDFSUtil with DateUtil {

  implicit val materializer = ActorMaterializer()(system)
  implicit val ec = system.dispatcher

  protected implicit val configHadoop: Configuration = new Configuration()
  protected implicit val fs: FileSystem = FileSystem.get(configHadoop)


  "path with filter" in {
    val request = OccurrenceRequest(OccurrenceSelector("Aves|Mammalia", "some wkt", "some trait"),
      added = DateTimeSelector(before = Some("2017-05-01"), after = Some("2016-02-03")))

    val pattern: FilePattern = selectPathByDateRange(request, "some/path")

    pattern.filter(new Path("some/path/y=2017/m=04/d=04")) shouldBe true
    pattern.filter(new Path("some/path/y=2018/m=04/d=04")) shouldBe false
    pattern.filter(new Path("some/path/y=2015/m=04/d=04")) shouldBe false
  }

  "path with no date filter" in {
    val request = OccurrenceRequest(OccurrenceSelector("Aves|Mammalia", "some wkt", "some trait"))

    val pattern: FilePattern = selectPathByDateRange(request, "some/path")

    pattern.filter(new Path("some/path/y=2017/m=04/d=04")) shouldBe true
    pattern.filter(new Path("some/path/y=2018/m=04/d=04")) shouldBe true
    pattern.filter(new Path("some/path/y=2015/m=04/d=04")) shouldBe true
  }

  "path with before date filter" in {
    val request = OccurrenceRequest(OccurrenceSelector("Aves|Mammalia", "some wkt", "some trait"),
    added = DateTimeSelector(before = Some("2017-06-01")))

    val pattern: FilePattern = selectPathByDateRange(request, "some/path")

    pattern.filter(new Path("some/path/y=2017/m=04/d=04")) shouldBe true
    pattern.filter(new Path("some/path/y=2018/m=04/d=04")) shouldBe false
    pattern.filter(new Path("some/path/y=2015/m=04/d=04")) shouldBe true
  }

  "path with after date filter" in {
    val request = OccurrenceRequest(OccurrenceSelector("Aves|Mammalia", "some wkt", "some trait"),
    added = DateTimeSelector(after = Some("2016-06-01")))

    val pattern: FilePattern = selectPathByDateRange(request, "some/path")

    pattern.filter(new Path("some/path/y=2017/m=04/d=04")) shouldBe true
    pattern.filter(new Path("some/path/y=2018/m=04/d=04")) shouldBe true
    pattern.filter(new Path("some/path/y=2015/m=04/d=04")) shouldBe false
  }


  private def selectPathByDateRange(request: OccurrenceRequest, path: String) = {
    val datePathPattern = "(.*)/y=([0-9]{4})/m=([0-9]{2})/d=([0-9]{2})".r
    FilePattern(path + "/*")
      .withFilter({ x => {
        x.toUri.toString match {
          case datePathPattern(_, year, month, day) => {
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
      })
  }
}