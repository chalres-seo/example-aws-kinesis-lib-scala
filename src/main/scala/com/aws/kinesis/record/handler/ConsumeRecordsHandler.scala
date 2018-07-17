package com.aws.kinesis.record.handler

import com.amazonaws.services.kinesis.model.Record
import com.aws.kinesis.record.RecordImpl
import com.typesafe.scalalogging.LazyLogging

trait ConsumeRecordsHandler extends LazyLogging {
  type KinesisRecordsHandlerType = Vector[Record] => Unit
  type RecordsHandlerType[T] = Vector[RecordImpl[T]] => Unit

  def printStdout[T](records: Vector[T]): Unit = {
    logger.debug(s"start print stdout handler. record count: ${records.size}")
    records.foreach(println)
  }

  def debugStdout[T](records: Vector[T]): Unit = {
    logger.debug(s"start debug stdout handler. record count: ${records.size}")
    records.foreach(record => logger.debug(record.toString))
  }
}

object ConsumeRecordsHandler extends ConsumeRecordsHandler
