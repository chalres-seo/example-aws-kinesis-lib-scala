package com.aws.kinesis.record

import java.nio.ByteBuffer

import com.amazonaws.services.kinesis.model.Record

trait RecordImpl[T] {
  def getPartitionKey: String
  def getData: T
  def getByteBuffer: Option[ByteBuffer]
  def getSequenceNumber: Option[String]
  def getKinesisRecord: Record
}