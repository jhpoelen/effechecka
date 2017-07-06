package effechecka

import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import io.eels.component.parquet.ParquetSource
import io.eels.{CloseableIterator, FilePattern, Row}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

trait EelRowIterator {

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

trait EelRowSource extends GraphStage[SourceShape[Row]] with EelRowIterator {

  implicit val filePattern: Option[FilePattern]
  implicit val limit: Option[Int]

  // Define the (sole) output port of this stage
  val out: Outlet[Row] = Outlet("RowSource")
  // Define the shape of this stage, which is SourceShape with the port we defined above
  override val shape: SourceShape[Row] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      private val items = getRows(filePattern, limit)

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          println("onPull")
          try {
            if (items.iterator.hasNext) {
              val line = items.iterator.next()
              push(out, line)
            } else {
              if (!items.isClosed()) {
                items.close()
              }
              complete(out)
            }
          } catch {
            case e: Throwable => {
              if (!items.isClosed()) {
                items.close()
              }
              fail(out, e)
            }
          }
        }
        override def onDownstreamFinish: Unit = {
          if (!items.isClosed()) {
            items.close()
          }
        }
      })
    }
}