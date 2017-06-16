package effechecka

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{Matchers, WordSpecLike}

trait ConfigureTest {
  def config = ConfigFactory.load("effechecka/spark-submit-test.conf")
}

class SparkSubmitterSpec extends TestKit(ActorSystem("SparkIntegrationTest"))
  with WordSpecLike
  with Matchers
  with ScalaFutures
  with ConfigureTest
  with SparkSubmitter {

  implicit val materializer = ActorMaterializer()(system)
  implicit val ec = system.dispatcher
  implicit val defaultPatience = PatienceConfig(timeout = Span(10, Seconds), interval = Span(500, Millis))

  "submit checklist job" in {
    val selector: OccurrenceSelector = OccurrenceSelector("Animalia|Insecta", "ENVELOPE(-150,-50,40,10)", "")
    assertOkAndLog(requestChecklist(selector))
  }

  "submit occurrence job" in {
    val selector: OccurrenceSelector = OccurrenceSelector("Animalia|Insecta", "ENVELOPE(-150,-50,40,10)", "")
    assertOkAndLog(requestOccurrences(selector))
  }

  "update monitors" in {
    assertOkAndLog(requestUpdateAll())
  }

  private def assertOkAndLog(req: HttpRequest) = {
    val resp = Http().singleRequest(req)
    whenReady(resp) { result =>
      result.status.intValue() should be(200)
      println(result.entity.toString)
    }
  }


}