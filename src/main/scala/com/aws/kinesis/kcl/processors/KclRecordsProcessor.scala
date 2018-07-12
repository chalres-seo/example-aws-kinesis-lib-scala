package com.aws.kinesis.kcl.processors

import com.amazonaws.services.kinesis.model.Record
import com.aws.kinesis.record.handler.ConsumeRecordsHandler
import com.typesafe.scalalogging.LazyLogging

class KclRecordsProcessor(handler: ConsumeRecordsHandler.RecordsHandlerType) extends LazyLogging with IKclRecordsProcessor {
  override def processRecordsWithRetries(records: Vector[Record]): Unit = {
    handler(records)
  }
}