package effechecka

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethods, HttpRequest}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.typesafe.config.Config
import effechecka.selector.OccurrenceSelector

import scala.concurrent.ExecutionContext

trait SparkSubmitter {

  implicit def config: Config

  implicit val system: ActorSystem
  implicit val materializer: ActorMaterializer
  implicit val ec: ExecutionContext

  def submitOccurrenceCollectionsRefreshRequest(): Unit = {
    send(requestUpdateAll())
  }

  def submitOccurrenceCollectionRequest(selector: OccurrenceSelector, persistence: String = "cassandra"): Unit = {
    send(requestOccurrences(selector, persistence))
  }

  def submitChecklistRequest(checklist: ChecklistRequest, persistence: String = "cassandra"): Unit = {
    send(requestChecklist(checklist.selector, persistence))
  }

  private def send(req: HttpRequest) = {
    Source(List(req))
      .runWith(Sink.foreach[HttpRequest](Http().singleRequest(_)))
      .foreach(resp => println(resp))
  }

  def requestChecklist(selector: OccurrenceSelector, persistence: String = "cassandra"): HttpRequest = requestFor(argsFor(selector), "ChecklistGenerator", persistence)
  def requestOccurrences(selector: OccurrenceSelector, persistence: String = "cassandra"): HttpRequest = requestFor(argsFor(selector), "OccurrenceCollectionGenerator", persistence)
  def requestUpdateAll(): HttpRequest = requestFor(""""-a", "true"""", "OccurrenceCollectionGenerator")


  private def argsFor(selector: OccurrenceSelector) = {
    val argSelectorTaxon = selector.taxonSelector.replace(',', '|')
    val argSelectorWktString = selector.wktString
    val argSelectorTrait = selector.traitSelector.replace(',', '|')
    List(argSelectorTaxon, argSelectorWktString, argSelectorTrait).map(""""\"""" + _ + """\""""").mkString(", ")
  }

  def requestFor(args: String, sparkJobMainClass: String, persistence: String = "cassandra") = {
    val sparkJobJar = config.getString("effechecka.spark.job.jar")
    val dataPathOccurrences = config.getString("effechecka.data.dir") + "gbif-idigbio.parquet"
    val dataPathTraits = config.getString("effechecka.data.dir") + "traitbank/*.csv"
    val sparkJobRequest = s"""{
                             |      "action" : "CreateSubmissionRequest",
                             |      "appArgs" : [ "-f", "$persistence","-c","\\"$dataPathOccurrences\\"","-t", "\\"$dataPathTraits\\"", $args],
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
                             |        "spark.cassandra.connection.host" : "${config.getString("effechecka.cassandra.host")}",
                             |        "spark.app.name" : "$sparkJobMainClass",
                             |        "_spark.eventLog.enabled": "true",
                             |        "spark.submit.deployMode" : "cluster",
                             |        "spark.master" : "${config.getString("effechecka.spark.master.url")}",
                             |        "spark.executor.memory" : "${config.getString("effechecka.spark.executor.memory")}",
                             |        "spark.driver.memory" : "${config.getString("effechecka.spark.driver.memory")}",
                             |        "spark.task.maxFailures" : 1
                             |      }
                             |    }""".stripMargin

    println(sparkJobRequest)
    val payload = HttpEntity(contentType = ContentTypes.`application/json`, string = sparkJobRequest)
    val uri = s"""http://${config.getString("effechecka.spark.master.host")}:${config.getString("effechecka.spark.master.port")}/v1/submissions/create"""
    HttpRequest(uri = uri, method = HttpMethods.POST, entity = payload)
  }


}
