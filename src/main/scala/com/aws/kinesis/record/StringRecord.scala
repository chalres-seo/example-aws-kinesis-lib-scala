package com.aws.kinesis.record

import java.nio.{ByteBuffer, CharBuffer}
import java.nio.charset._

import com.amazonaws.services.kinesis.model.Record

import scala.util.Try

case class StringRecord(partitionKey: String, data: String, sequenceNumber: Option[String])
  extends RecordImpl[String] {

  private val byteBuffer = StringRecord.stringToByteBuffer(data)

  override def getPartitionKey: String = partitionKey
  override def getData: String = data

  // Produce record is None.
  override def getSequenceNumber: Option[String] = sequenceNumber

  @throws(classOf[CharacterCodingException])
  override def getByteBuffer: Try[ByteBuffer] = {
    byteBuffer.foreach(_.rewind())
    byteBuffer
  }

  override def getKinesisRecord: Record = sequenceNumber match {
    case Some(seq) =>
      new Record()
        .withSequenceNumber(seq)
        .withPartitionKey(partitionKey)
        .withData(this.getByteBuffer.get)
    case None =>
      new Record()
        .withPartitionKey(partitionKey)
        .withData(this.getByteBuffer.get)
  }
}

object StringRecord {
  private val encoder: CharsetEncoder = StandardCharsets.UTF_8.newEncoder()
  private val decoder: CharsetDecoder = StandardCharsets.UTF_8.newDecoder()

  def apply(partitionKey: String, data: String): StringRecord =
    new StringRecord(partitionKey, data, Option.empty[String])

  def apply(partitionKey: String, data: String, sequenceNumber: String): StringRecord =
    new StringRecord(partitionKey, data, Option(sequenceNumber))

  @throws(classOf[CharacterCodingException])
  def apply(partitionKey: String, data: ByteBuffer): Try[StringRecord] =
    Try(new StringRecord(partitionKey, StringRecord.byteBufferToString(data).get, Option.empty[String]))

  @throws(classOf[CharacterCodingException])
  def apply(record: Record): Try[StringRecord] = {
    Try(new StringRecord(record.getPartitionKey, StringRecord.byteBufferToString(record.getData).get, Option(record.getSequenceNumber)))
  }

  @throws(classOf[CharacterCodingException])
  def byteBufferToString(byteBuffer: ByteBuffer): Try[String] = Try(decoder.decode(byteBuffer).toString)

  @throws(classOf[CharacterCodingException])
  def stringToByteBuffer(s: String): Try[ByteBuffer] = Try(encoder.encode(CharBuffer.wrap(s)))

  def createExampleRecords(recordCount: Int): Vector[StringRecord] =
    (1 to recordCount).map(index => this(s"pk-$index", s"data-$index")).toVector
}
