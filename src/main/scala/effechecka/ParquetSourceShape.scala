package effechecka

import java.util.concurrent.CountDownLatch

import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.sksamuel.exts.Logging
import io.eels.component.parquet.ParquetSource
import io.eels.schema.StructType
import io.eels.{CloseableIterator, FilePattern, Row}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

trait ParquetIterator {

  protected implicit val configHadoop: Configuration
  protected implicit val fs: FileSystem

  def getRows(filePattern: Option[FilePattern], limit: Option[Int]): CloseableIterator[Row] = {
    filePattern match {
      case Some(path) =>
        val source = ParquetSource(path)
        if (source.parts().isEmpty) CloseableIterator.empty else {
          val i = source.toFrame().rows()
          limit match {
            case Some(aLimit) => i.take(aLimit)
            case _ => i
          }
        }
      case None => CloseableIterator.empty
    }
  }
}

class ParquetSourceShape
  (filePattern: Option[FilePattern], limit: Option[Int])
  (implicit val configHadoop: Configuration, implicit val fs: FileSystem)

  extends GraphStage[SourceShape[Row]]
    with ParquetIterator with Logging {

  // Define the (sole) output port of this stage
  val out: Outlet[Row] = Outlet("RowSource")
  // Define the shape of this stage, which is SourceShape with the port we defined above
  override val shape: SourceShape[Row] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      private val items = getRows(filePattern, limit)
      private val countDownLatch = new CountDownLatch(limit.getOrElse(0))
      setHandler(out, new OutHandler {
        def close(): Unit = {
          logger.info("attempting to close stream")
          if (!items.isClosed()) {
            items.close()
          }
        }

        override def onPull(): Unit = {
          try {
            if (items.iterator.hasNext) {
              val line = items.iterator.next()
              push(out, line)
            } else {
              close()
              complete(out)
            }
            println(s"count [${countDownLatch.getCount}]")
            countDownLatch.countDown()
          } catch {
            case e: Throwable => {
              close()
              fail(out, e)
            }
          }
        }
      })
    }
}