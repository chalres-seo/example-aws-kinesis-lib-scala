package com.aws.kinesis.record.handler

import java.io.FileOutputStream
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}

import com.amazonaws.services.kinesis.model.Record
import com.aws.kinesis.record.{RecordImpl, StringRecord}
import com.typesafe.scalalogging.LazyLogging
import com.utils.AppUtils

import scala.util.{Failure, Success, Try}

object RecordsHandler extends LazyLogging with ConsumeRecordsHandler {
  def printStdout[T](records: Vector[Record]): Unit = {
    logger.debug(s"start print stdout handler. record count: ${records.size}")
    records.foreach(println)
  }

  def debugStdout[T](records: Vector[Record]): Unit = {
    logger.debug(s"start debug stdout handler. record count: ${records.size}")
    records.foreach(record => logger.debug(record.toString))
  }

  def printData[T](records: Vector[Record]): Unit = {
    logger.debug(s"start print data handler. record count: ${records.size}")
    records.foreach(record => {
      record.getData.rewind()
      StringRecord.byteBufferToString(record.getData) match {
        case Success(data) => println(data)
        case Failure(t: Throwable) =>
          logger.error("failed convert kinesis record byte data to string.")
          logger.error(s"skkiped record. record: $record")
          logger.error(t.getMessage, t)
      }
      record.getData.rewind()
    })
  }

  def debugData[T](records: Vector[Record]): Unit = {
    logger.debug(s"start debug data handler. record count: ${records.size}")
    records.foreach(record => {
      record.getData.rewind()
      StringRecord.byteBufferToString(record.getData) match {
        case Success(data) => logger.debug(data)
        case Failure(t: Throwable) =>
          logger.error("failed convert kinesis record byte data to string.")
          logger.error(s"skkiped record. record: $record")
          logger.error(t.getMessage, t)
      }
      record.getData.rewind()
    })
  }

  def tmpFileout[T](filePathString: String, append: Boolean, records: Vector[Record]): Unit = {
    logger.debug(s"start tmp file out handler. record count: ${records.size}")

    AppUtils.checkDirAndIfNotExistCreate(filePathString)

    val newLine: Array[Byte] = "\n".getBytes(StandardCharsets.UTF_8)
    val fileOutputStream: FileOutputStream = new FileOutputStream(filePathString, append)

    records.foreach(record => {
      Try {
        record.getData.rewind()
        fileOutputStream.write(record.getData.array())
        fileOutputStream.write(newLine)
        record.getData.rewind()
      } match {
        case Success(_) =>
          logger.debug(s"succeed write to tmp file. file: $filePathString, record: $record")
        case Failure(t: Throwable) =>
          logger.error(t.getMessage, t)
          logger.error(s"failed write to tmp file. file: $filePathString, record: $record")
      }
    })

    fileOutputStream.flush()
    fileOutputStream.close()
  }
}