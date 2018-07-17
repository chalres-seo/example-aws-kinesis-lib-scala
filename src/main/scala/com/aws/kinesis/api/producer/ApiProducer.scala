package com.aws.kinesis.api.producer

import com.amazonaws.services.kinesis.model._
import com.aws.kinesis.api.ApiClient
import com.aws.kinesis.record.RecordImpl
import com.typesafe.scalalogging.LazyLogging
import com.utils.KinesisRetry

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

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
    * Produce records.
    *
    * @param records put records.
    *
    * @tparam T produce records data type.
    *
    * @throws ResourceNotFoundException there is no stream to put records.
    * @throws InvalidArgumentException invalid argument error to put records.
    * @throws ProvisionedThroughputExceededException available throughput capacity error.
    * @throws KMSDisabledException KMS error when putting records
    * @throws KMSInvalidStateException KMS error when putting records
    * @throws KMSAccessDeniedException KMS error when putting records
    * @throws KMSNotFoundException KMS error when putting records
    * @throws KMSOptInRequiredException KMS error when putting records
    * @throws KMSThrottlingException KMS error when putting records
    *
    * @return put record result.
    */
  @throws(classOf[Exception])
  def produce[T](records: Vector[RecordImpl[T]]): Future[Boolean] = {
    logger.debug(s"produce records to stream. name: $streamName, count: ${records.size}")

    val putRecordsRequest = new PutRecordsRequest().withStreamName(streamName)

    @tailrec
    def loop(currentPutRecordsRequestEntryList: Vector[PutRecordsRequestEntry]): Boolean = {
      putRecordsRequest.setRecords(currentPutRecordsRequestEntryList.asJava)

      apiClient.putRecords(putRecordsRequest) match {
        case Success(putRecordsResult) =>
          if (putRecordsResult.getFailedRecordCount > 0) {
            val failedPutRecordsRequestEntryList = this.getFailedPutRecordsRequestEntry(putRecordsResult, currentPutRecordsRequestEntryList)

            logger.debug(s"back-off and failed produce records re-produce. count: ${failedPutRecordsRequestEntryList.size}")
            KinesisRetry.backoff()
            loop(failedPutRecordsRequestEntryList)
          } else true
        case Failure(t: Throwable) =>
          logger.error(s"failed put records, stream: $streamName")
          logger.error(s"skipped records. count : ${currentPutRecordsRequestEntryList.size}")
          false
      }
    }

    Future(loop(records.map(this.toPutRecordsRequestEntry)))
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
  def apply(streamName: String): ApiProducer = this.apply(ApiClient(), streamName)
}
