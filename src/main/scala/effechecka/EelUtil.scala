package effechecka

import io.eels.{CloseableIterator, FilePattern, Row}
import io.eels.component.parquet.ParquetSource
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

trait EelUtil {
  protected implicit val configHadoop: Configuration
  protected implicit val fs: FileSystem

  def rows(path: FilePattern, limit: Option[Int]): Option[CloseableIterator[Row]] = {
    val source = ParquetSource(path)
    val rowIter = if (source.parts().isEmpty) None else {
      val i = source.toFrame().rows()
      Some(limit match {
        case Some(aLimit) => i.take(aLimit)
        case _ => i
      })
    }
    rowIter
  }

}
