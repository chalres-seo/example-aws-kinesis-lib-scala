package com.aws.kinesis.record.handler

import com.amazonaws.services.kinesis.model.Record

trait ConsumeRecordsHandler {
  type RecordsHandlerType = Vector[Record] => Unit
}

object ConsumeRecordsHandler extends ConsumeRecordsHandler
