package com.aws.kinesis.record.handler

import java.io.OutputStream
import java.nio.file.{Files, Paths, StandardOpenOption}

import com.aws.kinesis.record.RecordImpl
import com.typesafe.scalalogging.LazyLogging
import com.utils.AppUtils

import scala.util.{Failure, Success, Try}

object RecordHandler extends LazyLogging with ConsumeRecordsHandler {
  private val newLineByteBuffer = "\n".getBytes

  def tmpFileout[T](records: Vector[RecordImpl[T]], filePathString: String, standardOpenOption: StandardOpenOption*): Unit = {
    logger.debug(s"start tmp file out handler. record count: ${records.size}")

    if (!AppUtils.checkDirAndIfNotExistCreate(filePathString)) {
      logger.error(s"failed check dir. skipped records process. dir: $filePathString, record count: ${records.size}")
      return
    }

    val outputStream: OutputStream = Files.newOutputStream(Paths.get(filePathString), standardOpenOption:_*)

    records.foreach(record => {
      Try {
        record.getByteBuffer match {
          case Some(byteBuffer) =>
            outputStream.write(byteBuffer.array())
            outputStream.write(newLineByteBuffer)
          case None =>
            logger.error(s"byte buffer is none. skip record, record: $record")
        }
      } match {
        case Success(_) =>
          logger.debug(s"succeed write to tmp file. file: $filePathString, record: $record")
        case Failure(t: Throwable) =>
          logger.error(s"failed write to tmp file. file: $filePathString, record: $record")
          logger.error(t.getLocalizedMessage)
          logger.error(t.getMessage, t)
      }
    })

    outputStream.flush()
    outputStream.close()
  }
}