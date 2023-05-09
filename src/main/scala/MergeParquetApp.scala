import cats.Show
import cats.effect.{IO, IOApp}
import com.github.mjakubowski84.parquet4s.Path
import com.github.mjakubowski84.parquet4s.parquet.*
import fs2.Stream
import fs2.io.file.Files

import scala.util.Random

object MergeParquetApp extends IOApp.Simple {

  private case class Data(id: Int, text: String)

  implicit private val showData: Show[Data] = Show.fromToString

  override def run: IO[Unit] = {
    Stream
      .resource(Files[IO].tempDirectory(None, "", None)) // create temp directory
      .map(fs2Path => Path(fs2Path.toNioPath)) // convert to path
      .flatMap { basePath =>
        val rowsPerFile = 5
        val noOfFiles = 3
        // create 3 stream each having 5 rows
        // write them as 3 separate parquet files
        // read each file back as stream
        // join streams to form a single stream
        // write that stream as single file
        Stream
          .range[IO, Int](start = 1, stopExclusive = noOfFiles + 1)
          .map { fileNo =>
            val filePath = toSrcFileName(basePath, fileNo)

            Stream
              .range[IO, Int](start = 0, stopExclusive = rowsPerFile)
              .map { localRowNumber =>
                // generate a stream of data
                val globalRowNumber = fileNo * rowsPerFile + localRowNumber
                Data(
                  id = globalRowNumber,
                  text = s"this is text for file $fileNo and row ${localRowNumber + 1}"
                )
              }
              .through { // write as parquet file to disk
                writeSingleFile[IO]
                  .of[Data]
                  .write(filePath)
              }
              .append { // read parquet file from disk
                fromParquet[IO]
                  .as[Data]
                  .read(filePath)
              }
          }
          .parJoinUnbounded // merge all streams to single stream, order is not preserved
          .through { // write as a single parquet file to disk
            writeSingleFile[IO]
              .of[Data]
              .write(toDestFileName(basePath))
          }
          .append { // read merged parquet file from disk
            fromParquet[IO]
              .as[Data]
              .read(toDestFileName(basePath))
          }
          .printlns
          .drain

      }.compile.drain
  }

  private def toSrcFileName(path: Path, fileNo: Int) = path.append(s"data$fileNo.parquet")
  private def toDestFileName(path: Path) = path.append(s"data_merged.parquet")
}