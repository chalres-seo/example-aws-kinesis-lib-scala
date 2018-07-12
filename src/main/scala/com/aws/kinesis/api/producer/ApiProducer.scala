package com.aws.kinesis.api.producer

import com.amazonaws.services.kinesis.model.{PutRecordsRequest, PutRecordsRequestEntry, PutRecordsResult}
import com.aws.kinesis.api.ApiClient
import com.aws.kinesis.record.{RecordImpl, StringRecord}
import com.typesafe.scalalogging.LazyLogging
import com.utils.KinesisRetry

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success, Try}

/**
  * AWS SDK kinesis producer.
  *
  * @see [[com.aws.kinesis.record]]
  *
  * @param apiClient aws sdk kinesis client.
  * @param streamName checked stream name.
  */
class ApiProducer(apiClient: ApiClient, streamName: String) extends LazyLogging {

  def getStreamName: String = streamName

  /**
    *
    * @param records produce records
    * @tparam T record data type
    * @return produce records future
    */
  def produce[T](records: Vector[RecordImpl[T]]): Future[Boolean] = {
    logger.debug(s"produce records to stream. name: $streamName, count: ${records.size}")

    val putRecordsRequestEntryList: Vector[PutRecordsRequestEntry] = records.map(this.toPutRecordsRequestEntry)
    val putRecordsRequest = new PutRecordsRequest()
      .withStreamName(streamName)
      .withRecords(putRecordsRequestEntryList:_*)

    val putRecordsResult: Try[PutRecordsResult] = Try(apiClient.putRecords(putRecordsRequest))

    @tailrec
    def loop(currentPutRecordsResult: Try[PutRecordsResult], currentPutRecordsRequestEntryList: Vector[PutRecordsRequestEntry]): Boolean = {
      currentPutRecordsResult match {
        case Success(result) =>
          if (result.getFailedRecordCount > 0) {
            val failedPutRecordsRequestEntryList: Vector[PutRecordsRequestEntry] =
              this.getFailedPutRecordsRequestEntry(result, currentPutRecordsRequestEntryList)

            logger.debug(s"back-off and failed produce records re-produce. count: ${failedPutRecordsRequestEntryList.size}")
            KinesisRetry.backoff()

            putRecordsRequest.setRecords(failedPutRecordsRequestEntryList.asJava)
            loop(Try(apiClient.putRecords(putRecordsRequest)), failedPutRecordsRequestEntryList)
          } else true
        case Failure(throwable) =>
          logger.error(s"put records fail. msg : ${throwable.getMessage}")
          logger.error(s"skipped records. count : ${currentPutRecordsRequestEntryList.size}")
          currentPutRecordsRequestEntryList.foreach(entry =>
            logger.error(s"record(partitionKey=${entry.getPartitionKey}, " +
              s"data=${StringRecord.byteBufferToString(entry.getData)})"))
          false
      }
    }
    Future(loop(putRecordsResult, putRecordsRequestEntryList))
  }

  private def getFailedPutRecordsRequestEntry(putRecordsResult: PutRecordsResult,
                                              putRecordsRequestEntryList: Vector[PutRecordsRequestEntry]): Vector[PutRecordsRequestEntry] = {
    putRecordsRequestEntryList.zip(putRecordsResult
      .getRecords
      .asScala
      .map(_.getErrorCode != null)
    ).filter(_._2).map(_._1)
  }

  private def toPutRecordsRequestEntry[T](record: RecordImpl[T]): PutRecordsRequestEntry = {
    new PutRecordsRequestEntry()
      .withPartitionKey(record.getPartitionKey)
      .withData(record.getByteBuffer.get)
  }
}

object ApiProducer extends LazyLogging {
  @throws(classOf[IllegalArgumentException])
  def apply(apiClient: ApiClient, streamName: String): ApiProducer = {
    logger.debug(s"construct api producer. stream name: $streamName")

    require(apiClient.checkStreamExistAndReady(streamName), s"The stream must be present and ready. name: $streamName")
    new ApiProducer(apiClient, streamName)
  }

  @throws(classOf[IllegalArgumentException])
  def apply(streamName: String): ApiProducer = this(ApiClient(), streamName)
}
