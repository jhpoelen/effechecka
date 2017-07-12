package effechecka

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import com.sksamuel.exts.Logging
import com.typesafe.config.Config

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

trait JobSubmitter {
  def submit(selector: SelectorParams, jobMainClass: String): SparkDispatchResponse
}

trait SparkSubmitter extends JobSubmitter
  with Protocols
  with Logging {

  implicit def config: Config

  implicit val system: ActorSystem
  implicit val materializer: ActorMaterializer
  implicit val ec: ExecutionContext

  def submitOccurrenceCollectionsRefreshRequest(): Unit = {
    send(requestUpdateAll())
  }

  def submitOccurrenceCollectionRequest(selector: SelectorParams, persistence: String = "hdfs"): Unit = {
    send(requestOccurrences(selector, persistence))
  }

  def submitChecklistRequest(selector: SelectorParams, persistence: String = "hdfs"): Unit = {
    send(requestChecklist(selector, persistence))
  }

  protected def parse(response: HttpResponse): Future[SparkDispatchResponse] = {
    Unmarshal(response.entity).to[SparkDispatchResponse]
  }


  private def send(req: HttpRequest): SparkDispatchResponse = {
    val resp = Http().singleRequest(req).flatMap(response => parse(response))
    Await.result(resp, 10.seconds)
  }


  override def submit(selector: SelectorParams, jobMainClass: String): SparkDispatchResponse = send(requestFor(args = argsFor(selector), sparkJobMainClass = jobMainClass))

  def requestChecklist(selector: SelectorParams, persistence: String = "hdfs"): HttpRequest = requestFor(argsFor(selector), "ChecklistGenerator", persistence)

  def requestOccurrences(selector: SelectorParams, persistence: String = "hdfs"): HttpRequest = requestFor(argsFor(selector), "OccurrenceCollectionGenerator", persistence)

  def requestUpdateAll(): HttpRequest = requestFor(""""-a", "true"""", "OccurrenceCollectionGenerator")


  private def argsFor(selector: SelectorParams) = {
    val argSelectorTaxon = selector.taxonSelector.replace(',', '|')
    val argSelectorWktString = selector.wktString
    val argSelectorTrait = selector.traitSelector.replace(',', '|')
    List(argSelectorTaxon, argSelectorWktString, argSelectorTrait).map(""""\"""" + _ + """\""""").mkString(", ")
  }

  def requestFor(args: String, sparkJobMainClass: String, persistence: String = "hdfs") = {
    val sparkJobJar = config.getString("effechecka.spark.job.jar")
    val dataPathOccurrences = config.getString("effechecka.data.dir") + "/gbif-idigbio.parquet"
    val dataPathTraits = config.getString("effechecka.data.dir") + "/traitbank/*.csv"
    val outputPath = config.getString("effechecka.monitor.dir")
    val sparkJobRequest =
      s"""{
         |      "action" : "CreateSubmissionRequest",
         |      "appArgs" : [ "-f", "$persistence","-o", "\\"$outputPath\\"","-c","\\"$dataPathOccurrences\\"","-t", "\\"$dataPathTraits\\"", $args],
         |      "appResource" : "$sparkJobJar",
         |      "clientSparkVersion" : "2.0.1",
         |      "environmentVariables" : {
         |        "SPARK_ENV_LOADED" : "1",
         |        "HADOOP_HOME" : "/usr/lib/hadoop",
         |        "HADOOP_PREFIX" : "/usr/lib/hadoop",
         |        "HADOOP_LIBEXEC_DIR" : "/usr/lib/hadoop/libexec",
         |        "HADOOP_CONF_DIR" : "/etc/hadoop/conf",
         |        "HADOOP_USER_NAME" : "hdfs"
         |      },
         |      "mainClass" : "$sparkJobMainClass",
         |      "sparkProperties" : {
         |        "spark.driver.supervise" : "false",
         |        "spark.mesos.executor.home" : "${config.getString("effechecka.spark.mesos.executor.home")}",
         |        "spark.app.name" : "$sparkJobMainClass",
         |        "_spark.eventLog.enabled": "true",
         |        "spark.submit.deployMode" : "cluster",
         |        "spark.master" : "${config.getString("effechecka.spark.master.url")}",
         |        "spark.executor.memory" : "${config.getString("effechecka.spark.executor.memory")}",
         |        "spark.driver.memory" : "${config.getString("effechecka.spark.driver.memory")}",
         |        "spark.task.maxFailures" : 1
         |      }
         |    }""".stripMargin

    logger.info(sparkJobRequest)
    val payload = HttpEntity(contentType = ContentTypes.`application/json`, string = sparkJobRequest)
    val uri = s"""http://${config.getString("effechecka.spark.master.host")}:${config.getString("effechecka.spark.master.port")}/v1/submissions/create"""
    HttpRequest(uri = uri, method = HttpMethods.POST, entity = payload)
  }


}
