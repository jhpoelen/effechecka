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

  "checklist job request" in {
    val selector: OccurrenceSelector = OccurrenceSelector("Animalia|Insecta", "ENVELOPE(-150,-50,40,10)", "")
    val someRequest = requestChecklist(selector, "hdfs")
    val expectedRequestBody =
      """{
        |      "action" : "CreateSubmissionRequest",
        |      "appArgs" : [ "-f", "cassandra","-c","\"file:///does/not/exist/gbif-idigbio.parquet\"","-t", "\"file:///does/not/exist/traitbank/*.csv\"", "\"Animalia|Insecta\"", "\"ENVELOPE(-150,-50,40,10)\"", "\"\""],
        |      "appResource" : "file:///doesnotexist.jar",
        |      "clientSparkVersion" : "2.0.1",
        |      "environmentVariables" : {
        |        "SPARK_ENV_LOADED" : "1",
        |        "HADOOP_HOME" : "/usr/lib/hadoop",
        |        "HADOOP_PREFIX" : "/usr/lib/hadoop",
        |        "HADOOP_LIBEXEC_DIR" : "/usr/lib/hadoop/libexec",
        |        "HADOOP_CONF_DIR" : "/etc/hadoop/conf",
        |        "HADOOP_USER_NAME" : "hdfs"
        |      },
        |      "mainClass" : "ChecklistGenerator",
        |      "sparkProperties" : {
        |        "spark.driver.supervise" : "false",
        |        "spark.mesos.executor.home" : "/path/to/local/spark/on/mesos/worker",
        |        "spark.cassandra.connection.host" : "localhost",
        |        "spark.app.name" : "ChecklistGenerator",
        |        "_spark.eventLog.enabled": "true",
        |        "spark.submit.deployMode" : "cluster",
        |        "spark.master" : "mesos://api.effechecka.org:7077",
        |        "spark.executor.memory" : "32g",
        |        "spark.driver.memory" : "8g",
        |        "spark.task.maxFailures" : 1
        |      }
        |    }""".stripMargin

    assertRequest(someRequest, expectedRequestBody)
  }

  "occurrence job request" in {
    val selector: OccurrenceSelector = OccurrenceSelector("Animalia|Insecta", "ENVELOPE(-150,-50,40,10)", "")
    val someRequest = requestOccurrences(selector)
    val expectedRequestBody =
      """{
        |      "action" : "CreateSubmissionRequest",
        |      "appArgs" : [ "-f", "cassandra","-c","\"file:///does/not/exist/gbif-idigbio.parquet\"","-t", "\"file:///does/not/exist/traitbank/*.csv\"", "\"Animalia|Insecta\"", "\"ENVELOPE(-150,-50,40,10)\"", "\"\""],
        |      "appResource" : "file:///doesnotexist.jar",
        |      "clientSparkVersion" : "2.0.1",
        |      "environmentVariables" : {
        |        "SPARK_ENV_LOADED" : "1",
        |        "HADOOP_HOME" : "/usr/lib/hadoop",
        |        "HADOOP_PREFIX" : "/usr/lib/hadoop",
        |        "HADOOP_LIBEXEC_DIR" : "/usr/lib/hadoop/libexec",
        |        "HADOOP_CONF_DIR" : "/etc/hadoop/conf",
        |        "HADOOP_USER_NAME" : "hdfs"
        |      },
        |      "mainClass" : "OccurrenceCollectionGenerator",
        |      "sparkProperties" : {
        |        "spark.driver.supervise" : "false",
        |        "spark.mesos.executor.home" : "/path/to/local/spark/on/mesos/worker",
        |        "spark.cassandra.connection.host" : "localhost",
        |        "spark.app.name" : "OccurrenceCollectionGenerator",
        |        "_spark.eventLog.enabled": "true",
        |        "spark.submit.deployMode" : "cluster",
        |        "spark.master" : "mesos://api.effechecka.org:7077",
        |        "spark.executor.memory" : "32g",
        |        "spark.driver.memory" : "8g",
        |        "spark.task.maxFailures" : 1
        |      }
        |    }""".stripMargin
    assertRequest(someRequest, expectedRequestBody)

  }

  "update monitor request" in {
    val someRequest = requestUpdateAll()
    val expectedRequestBody =
      """{
        |      "action" : "CreateSubmissionRequest",
        |      "appArgs" : [ "-f", "cassandra","-c","\"file:///does/not/exist/gbif-idigbio.parquet\"","-t", "\"file:///does/not/exist/traitbank/*.csv\"", "-a", "true"],
        |      "appResource" : "file:///doesnotexist.jar",
        |      "clientSparkVersion" : "2.0.1",
        |      "environmentVariables" : {
        |        "SPARK_ENV_LOADED" : "1",
        |        "HADOOP_HOME" : "/usr/lib/hadoop",
        |        "HADOOP_PREFIX" : "/usr/lib/hadoop",
        |        "HADOOP_LIBEXEC_DIR" : "/usr/lib/hadoop/libexec",
        |        "HADOOP_CONF_DIR" : "/etc/hadoop/conf",
        |        "HADOOP_USER_NAME" : "hdfs"
        |      },
        |      "mainClass" : "OccurrenceCollectionGenerator",
        |      "sparkProperties" : {
        |        "spark.driver.supervise" : "false",
        |        "spark.mesos.executor.home" : "/path/to/local/spark/on/mesos/worker",
        |        "spark.cassandra.connection.host" : "localhost",
        |        "spark.app.name" : "OccurrenceCollectionGenerator",
        |        "_spark.eventLog.enabled": "true",
        |        "spark.submit.deployMode" : "cluster",
        |        "spark.master" : "mesos://api.effechecka.org:7077",
        |        "spark.executor.memory" : "32g",
        |        "spark.driver.memory" : "8g",
        |        "spark.task.maxFailures" : 1
        |      }
        |    }""".stripMargin
    assertRequest(someRequest, expectedRequestBody)
  }

  private def assertRequest(someRequest: HttpRequest, expectedRequestBody: String) = {
    someRequest.uri shouldBe Uri("http://api.effechecka.org:7077/v1/submissions/create")
    someRequest.entity shouldBe HttpEntity.apply(ContentTypes.`application/json`, expectedRequestBody)
  }

}