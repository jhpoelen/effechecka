package effechecka

import java.io.File

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, SourceShape}
import akka.stream.scaladsl.{Flow, GraphDSL, Source}
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import akka.util.ByteString
import io.eels.{FilePattern, Row}
import org.apache.hadoop.fs.Path
import org.effechecka.selector.OccurrenceSelector
import org.scalatest.{Matchers, WordSpecLike}

class ParquetSourceShapeSpec extends TestKit(ActorSystem("IntegrationTest"))
  with WordSpecLike with Matchers with HDFSTestUtil with HDFSUtil {

  implicit val materializer = ActorMaterializer()(system)
  implicit val ec = system.dispatcher


  "parquet source shape" should {
    val ducksAndFrogsResourceString = "/hdfs-layout/occurrence-summary/u0=06/u1=ac/u2=07/uuid=06ac07ec-7d99-521b-a768-04ab6013ab27"
    val ducksAndFrogsResource = getClass.getResource(ducksAndFrogsResourceString)

    "have some expected test files" in {
      getClass.getResource("/hdfs-layout/occurrence/u0=55/u1=e4/u2=b0/uuid=55e4b0a0-bcd9-566f-99bc-357439011d85/occurrence.parquet") shouldNot be(null)
      ducksAndFrogsResource shouldNot be(null)
      getClass.getResource("/hdfs-layout/occurrence-summary/u0=05/u1=2f/u2=ec/uuid=052fec64-d7d8-5266-a4cd-119e3614831e") should be(null)
    }

    "return rows" in {
      val ducksAndFrogs = OccurrenceSelector("Anas|Anura", "POLYGON ((-72.147216796875 41.492120839687786, -72.147216796875 43.11702412135048, -69.949951171875 43.11702412135048, -69.949951171875 41.492120839687786, -72.147216796875 41.492120839687786))", "")
      val pathString = ducksAndFrogsResource.toURI.toString
      val baseDir = new File(getClass.getResource("/hdfs-layout/base.txt").toURI).getParentFile.getParentFile.toURI.toString
      val path = new Path(baseDir + ducksAndFrogsResourceString)
      println(path.toUri.toString)
      val someFilePattern = if (fs.exists(path)) {
        val resourcePath = fs.resolvePath(path)
        Some(FilePattern(resourcePath))
      } else {
        None
      }

      println(path)

      val graph = GraphDSL.create(new ParquetReaderSourceShape(someFilePattern, Some(1))) { implicit builder =>
        (parquetSource) =>
          import GraphDSL.Implicits._
          val toMonitoredOccurrences = Flow[Row].map(row => ByteString.fromString(row.get(0).toString))
          val out = builder.add(toMonitoredOccurrences)
          parquetSource ~> out
        SourceShape(out.out)
      }

      val probe = Source.fromGraph(graph)
        .runWith(TestSink.probe[ByteString])

      probe.request(3)
      probe.expectNext(ByteString("239543"))
      probe.expectComplete()
    }

  }


}
