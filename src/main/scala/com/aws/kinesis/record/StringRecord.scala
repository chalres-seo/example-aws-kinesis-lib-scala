package com.aws.kinesis.record

import java.nio.{ByteBuffer, CharBuffer}
import java.nio.charset._

import com.amazonaws.services.kinesis.model.Record
import com.typesafe.scalalogging.LazyLogging

import scala.util.{Failure, Try}

class StringRecord(partitionKey: String, data: String, sequenceNumber: Option[String])
  extends LazyLogging with RecordImpl[String] {

  private val byteBuffer: Option[ByteBuffer] = StringRecord.stringToByteBuffer(data).toOption

  override def getPartitionKey: String = partitionKey
  override def getData: String = data

  // Produce record is None.
  override def getSequenceNumber: Option[String] = sequenceNumber

  override def getByteBuffer: Option[ByteBuffer] = {
    if (byteBuffer.isDefined) byteBuffer.get.rewind()
    byteBuffer
  }

  override def getKinesisRecord: Record = {
    if (byteBuffer.isDefined) byteBuffer.get.rewind()

    new Record()
      .withSequenceNumber(sequenceNumber.orNull)
      .withPartitionKey(partitionKey)
      .withData(byteBuffer.orNull)
  }
}

object StringRecord extends LazyLogging {
  private val encoder: CharsetEncoder = StandardCharsets.UTF_8.newEncoder()
  private val decoder: CharsetDecoder = StandardCharsets.UTF_8.newDecoder()

  def apply(partitionKey: String, data: String, sequence: String): StringRecord = {
    new StringRecord(partitionKey, data, Option(sequence))
  }

  def apply(partitionKey: String, data: String): StringRecord = {
    new StringRecord(partitionKey, data, Option.empty[String])
  }

  @throws(classOf[CharacterCodingException])
  def apply(partitionKey: String, byteBuffer: ByteBuffer): Try[StringRecord] = {
    val buffer = byteBuffer.asReadOnlyBuffer()
    buffer.rewind()

    Try(new StringRecord(partitionKey, byteBufferToString(buffer).get, Option.empty[String]))
  }

  @throws(classOf[CharacterCodingException])
  def apply(record: Record): Try[StringRecord] = {
    val buffer = record.getData.asReadOnlyBuffer()
    buffer.rewind()

    Try(new StringRecord(record.getPartitionKey, byteBufferToString(buffer).get, Option(record.getSequenceNumber)))
  }

  @throws(classOf[CharacterCodingException])
  def byteBufferToString(byteBuffer: ByteBuffer): Try[String] = {
    Try(decoder.decode(byteBuffer).toString).recoverWith {
      case e: CharacterCodingException =>
        logger.error(s"failed decode byte buffer.")
        logger.error(e.getMessage)
        Failure(e)
      case t: Throwable =>
        logger.error(s"failed decode byte byffer. unknown exception.")
        logger.error(t.getMessage, t)
        Failure(t)
    }
  }

  @throws(classOf[CharacterCodingException])
  def stringToByteBuffer(s: String): Try[ByteBuffer] = {
    Try(encoder.encode(CharBuffer.wrap(s))).recoverWith {
      case e: CharacterCodingException =>
        logger.error(s"failed encode data. data: ${s}")
        logger.error(e.getMessage)
        Failure(e)
      case t: Throwable =>
        logger.error(s"failed encode data. unknown exception. data: ${s}")
        logger.error(t.getMessage, t)
        Failure(t)
    }
  }

  def createExampleRecords(recordCount: Int): Vector[StringRecord] =
    (1 to recordCount).map(index => this(s"pk-$index", s"data-$index")).toVector
}
