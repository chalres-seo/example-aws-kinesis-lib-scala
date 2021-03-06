package com.aws.kinesis.library.consumer.processors

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.{IRecordProcessor, IRecordProcessorFactory}
import com.aws.kinesis.record.handler.ConsumeRecordsHandler
import com.typesafe.scalalogging.LazyLogging

class KclRecordsProcessorFactory(handler: ConsumeRecordsHandler.KinesisRecordsHandlerType) extends LazyLogging with IRecordProcessorFactory {
  override def createProcessor(): IRecordProcessor = new KclRecordsProcessor(handler)
}