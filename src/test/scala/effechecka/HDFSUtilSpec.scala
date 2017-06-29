package effechecka

import java.util.{Date, UUID}

import akka.actor.ActorSystem
import akka.http.scaladsl.model.DateTime
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import effechecka.selector.{DateTimeSelector, OccurrenceSelector}
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

    pattern.filter(new Path("some/path/y=2017/m=04/d=04/bla")) shouldBe true
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

}