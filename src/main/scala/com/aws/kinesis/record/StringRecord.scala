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

  override def toString = s"StringRecord($getPartitionKey, $getData, $getSequenceNumber, $byteBuffer)"

  override def equals(other: Any): Boolean = other match {
    case that: StringRecord =>
        partitionKey == that.getPartitionKey &&
        data == that.getData &&
        byteBuffer == that.getByteBuffer
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(partitionKey, data, byteBuffer)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
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
    Try(new StringRecord(partitionKey, byteBufferToString(byteBuffer.asReadOnlyBuffer()).get, Option.empty[String]))
  }

  @throws(classOf[CharacterCodingException])
  def apply(record: Record): Try[StringRecord] = {
    Try(new StringRecord(record.getPartitionKey, byteBufferToString(record.getData.asReadOnlyBuffer).get, Option(record.getSequenceNumber)))
  }

  @throws(classOf[CharacterCodingException])
  def byteBufferToString(byteBuffer: ByteBuffer): Try[String] = {
    byteBuffer.rewind()

    Try(decoder.decode(byteBuffer).toString).recoverWith {
      case e: CharacterCodingException =>
        logger.error(s"failed decode byte buffer. byteBuffer: $byteBuffer")
        logger.error(e.getMessage)
        Failure(e)
      case t: Throwable =>
        logger.error(s"failed decode byte buffer. unknown exception. byteBuffer: $byteBuffer")
        logger.error(t.getMessage, t)
        Failure(t)
    }
  }

  @throws(classOf[CharacterCodingException])
  def stringToByteBuffer(s: String): Try[ByteBuffer] = {
    Try(encoder.encode(CharBuffer.wrap(s))).recoverWith {
      case e: CharacterCodingException =>
        logger.error(s"failed encode data. data: $s")
        logger.error(e.getMessage)
        Failure(e)
      case t: Throwable =>
        logger.error(s"failed encode data. unknown exception. data: $s")
        logger.error(t.getMessage, t)
        Failure(t)
    }
  }

  def recordsToStringRecords(records: Vector[Record]): Vector[StringRecord] = {
    for {
      record <- records.map(StringRecord(_))
      if record.isSuccess
    } yield record.get
  }

  def createExampleRecords(recordCount: Int): Vector[StringRecord] =
    (1 to recordCount).map(index => this(s"pk-$index", s"data-$index")).toVector
}
