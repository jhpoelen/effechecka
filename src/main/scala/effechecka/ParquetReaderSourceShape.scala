package effechecka

import java.util.concurrent.CountDownLatch

import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.sksamuel.exts.Logging
import io.eels.component.parquet.RowReadSupport
import io.eels.{FilePattern, Row}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.ParquetReader

import scala.annotation.tailrec


trait ParquetReaderIterator extends Logging {

  protected implicit val configHadoop: Configuration
  protected implicit val fs: FileSystem

  def getReaders(filePattern: Option[FilePattern], limit: Option[Int]): BufferedIterator[ParquetReader[Row]] = {
    filePattern match {
      case None => Iterator().buffered
      case Some(path) =>
        lazy val paths: List[Path] = path.toPaths()
        if (paths.isEmpty) Iterator().buffered else {
          paths.iterator.map { pathElem =>
            def configuration(): Configuration = {
              val conf = new Configuration()
              conf.set(org.apache.parquet.hadoop.ParquetFileReader.PARQUET_READ_PARALLELISM, "1")
              conf
            }

            if (logger.isDebugEnabled) {
              logger.debug(s"creating parquet reader for [$path]")
            }

            ParquetReader.builder(new RowReadSupport, pathElem)
              .withConf(configuration())
              .withFilter(FilterCompat.NOOP)
              .build()
          }.buffered
        }
    }
  }
}


class ParquetReaderSourceShape(filePattern: Option[FilePattern], limit: Option[Int])
                              (implicit val configHadoop: Configuration, implicit val fs: FileSystem)
  extends GraphStage[SourceShape[Row]]
    with ParquetReaderIterator with Logging {

  val out: Outlet[Row] = Outlet("RowSource")
  override val shape: SourceShape[Row] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      private val readers: BufferedIterator[ParquetReader[Row]] = getReaders(filePattern, limit)
      private val countDownLatch = new CountDownLatch(limit.getOrElse(0))
      setHandler(out, new OutHandler {

        def closeComplete(): Unit = {
          close()
          complete(out)
        }

        def close(): Unit = {
          logger.debug("attempting to close active reader")
          if (readers.nonEmpty) {
            readers.head.close()
          }
        }

        @tailrec final def tryNextRow(): Row = {
          if (readers.isEmpty) {
            null
          } else {
            val row = readers.head.read()
            if (row == null) {
              readers.head.close()
              readers.next()
              tryNextRow()
            } else {
              row
            }
          }
        }

        override def onPull(): Unit = {
          try {
            val r = if (limit.isEmpty || countDownLatch.getCount > 0) {
              tryNextRow()
            } else null

            if (r == null) {
              closeComplete()
            } else {
              push(out, r)
              if (logger.isDebugEnabled) {
                if (limit.isDefined) {
                  logger.debug(s"pushing row [${countDownLatch.getCount}]")
                } else {
                  logger.debug(s"pushing a row")
                }
              }
            }
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