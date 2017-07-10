package effechecka

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import io.eels.FilePattern
import org.apache.hadoop.fs.Path
import org.effechecka.selector.{DateTimeSelector, OccurrenceSelector}
import org.scalatest._


class HDFSUtilSpec extends TestKit(ActorSystem("IntegrationTest"))
  with WordSpecLike with Matchers with HDFSTestUtil with HDFSUtil with DateUtil {

  implicit val materializer = ActorMaterializer()(system)
  implicit val ec = system.dispatcher

  "path for selector" in {
    val selector = OccurrenceSelector("Aves|Mammalia", "some wkt", "some trait")

    val expectedPath = "u0=16/u1=c6/u2=29/uuid=16c62920-81fa-5216-aab1-bfda5cdfacde"
    pathForSelector(selector) should be(expectedPath)

    val selectorWithUUID = OccurrenceSelector(uuid = Some("16c62920-81fa-5216-aab1-bfda5cdfacde"))
    pathForSelector(selectorWithUUID) should be(expectedPath)

    val selectorWithUUIDAndMismatchingSelector = OccurrenceSelector(
      taxonSelector = "Aves|Mammalia",
      uuid = Some("16c62920-81fa-5216-aab1-bfda5cdfacde"))
    pathForSelector(selectorWithUUIDAndMismatchingSelector) should be(expectedPath)
  }

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