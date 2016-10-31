package effechecka

import com.typesafe.config.Config
import org.apache.spark.deploy.SparkSubmit

trait SparkSubmitter {

  implicit def config: Config

  val sparkSubmitParamsCore = Array("--master",
    config.getString("effechecka.spark.master.url"),
    "--deploy-mode", "cluster",
    "--java-driver-option", "log4j.properties",
    "--verbose",
    "--driver-memory", config.getString("effechecka.spark.driver.memory"),
    "--executor-memory", config.getString("effechecka.spark.executor.memory"))

  val sparkSubmitParams = sparkSubmitParamsCore ++ Array(
    "--class", "OccurrenceCollectionGenerator",
    config.getString("effechecka.spark.job.jar"),
    "-f", "cassandra",
    "-c", "\"" + config.getString("effechecka.data.dir") + "gbif-idigbio.parquet" + "\"",
    "-t", "\"" + config.getString("effechecka.data.dir") + "traitbank/*.csv" + "\"")


  def submitOccurrenceCollectionsRefreshRequest() = {
    SparkSubmit.main(sparkSubmitParams ++ Array("-a", "true"))
  }

  def submitOccurrenceCollectionRequest(selector: OccurrenceSelector) = {
    SparkSubmit.main(sparkSubmitParams ++ Array(
      "\"" + selector.taxonSelector.replace(',', '|') + "\"",
      "\"" + selector.wktString + "\"",
      "\"" + selector.traitSelector.replace(',', '|') + "\""))
  }

  def submitChecklistRequest(checklist: ChecklistRequest): Unit = {
    SparkSubmit.main(sparkSubmitParamsCore ++ Array(
      "--class", "ChecklistGenerator",
      config.getString("effechecka.spark.job.jar"),
      "-f", "cassandra",
      "-c", "\"" + config.getString("effechecka.data.dir") + "gbif-idigbio.parquet" + "\"",
      "-t", "\"" + config.getString("effechecka.data.dir") + "traitbank/*.csv" + "\"",
      "\"" + checklist.selector.taxonSelector.replace(',', '|') + "\"",
      "\"" + checklist.selector.wktString + "\"",
      "\"" + checklist.selector.traitSelector.replace(',', '|') + "\""))
  }

}
