package com.aws.kinesis.api.consumer


import com.amazonaws.services.kinesis.model._
import com.aws.kinesis.api.ApiClient
import com.aws.kinesis.record.handler.ConsumeRecordsHandler
import com.typesafe.scalalogging.LazyLogging
import com.utils.{AppConfig, KinesisRetry}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

/**
  * AWS SDK Kinesis consumer.
  *
  * The api consumer accepts function type(Vector[Record] => Unit) and processes it in consume-loop
  * Use function in [[com.aws.kinesis.record.handler]] or create custom function type.
  *
  * @see [[ConsumeRecordsHandler]]
  * @see [[com.aws.kinesis.record.handler]]
  *
  * @param apiClient  aws kinesis client.
  * @param streamName checked stream name.
  */
class ApiConsumer(apiClient: ApiClient, streamName: String) extends LazyLogging {
  private val DEFAULT_INTERVAL_MILLIS: Long = AppConfig.DEFAULT_INTERVAL_MILLIS
  private val DEFAULT_SHARD_ITERATOR_TYPE: ShardIteratorType = AppConfig.DEFAULT_SHARD_ITERATOR_TYPE

  def getStreamName: String = streamName

  /**
    * Start consumer list.
    * consumer count is equals to stream count.
    *
    * @param interValMillis consume interval millis.
    * @param shardIteratorType consume shard iterator type. [[ShardIteratorType]]
    * @param recordsHandler handler for the records process.
    *
    * @throws LimitExceededException exceeded request limit, retry and throw. [[KinesisRetry]].
    * @throws ResourceNotFoundException there is no stream to get records.
    *
    * @return consume loop future list
    */
  @throws(classOf[Exception])
  def consume(interValMillis: Long, shardIteratorType: ShardIteratorType)
             (recordsHandler: ConsumeRecordsHandler.RecordsHandlerType): Try[Vector[Future[Boolean]]] = {
    apiClient.getShardList(streamName).map(shardList => {
      shardList.map(shard => {
        consumeLoop(shard.getShardId, shardIteratorType, interValMillis)(recordsHandler)
      })
    })
  }

  def consume(recordsHandler: ConsumeRecordsHandler.RecordsHandlerType): Try[Vector[Future[Boolean]]] =
    consume(DEFAULT_INTERVAL_MILLIS, DEFAULT_SHARD_ITERATOR_TYPE)(recordsHandler)

  /**
    * Start single consumer.
    *
    * @param shardId shardId which consume loop.
    * @param shardIteratorType consume shard iterator type. [[ShardIteratorType]]
    * @param intervalMillis consume interval millis.
    * @param recordsHandler handler for the records process.
    *
    * @throws ResourceNotFoundException there is no stream to get records.
    * @throws InvalidArgumentException invalid argument error to get records.
    * @throws ProvisionedThroughputExceededException available throughput capacity error.
    * @throws KMSDisabledException KMS error when putting records
    * @throws KMSInvalidStateException KMS error when putting records
    * @throws KMSAccessDeniedException KMS error when putting records
    * @throws KMSNotFoundException KMS error when putting records
    * @throws KMSOptInRequiredException KMS error when putting records
    * @throws KMSThrottlingException KMS error when putting records
    *
    * @return single consume loop future, return true when next shard iterator is null, otherwise return false.
    */
  @throws(classOf[Exception])
  private def consumeLoop(shardId: String, shardIteratorType: ShardIteratorType, intervalMillis: Long)
                         (recordsHandler: ConsumeRecordsHandler.RecordsHandlerType): Future[Boolean] = {
    logger.debug(s"start consume loop. loop-iterator-shardId-$shardId")

    val getRecordsRequest: GetRecordsRequest = new GetRecordsRequest()

    @tailrec
    def loop(shardIterator: String): Boolean = {
      if (shardIterator == null) {
        logger.debug(s"shard iterator is null. stop consume-loop, stream: $streamName, loop-iterator-shardId-$shardId")
        return true
      }

      getRecordsRequest.setShardIterator(shardIterator)

      apiClient.getRecords(getRecordsRequest) match {
        case Success(getRecordsResult) =>
          val records = getRecordsResult.getRecords.asScala.toVector

          if (records.isEmpty) {
            logger.debug("consume records is null. go to next shardIterator")
          } else {
            recordsHandler(records)
          }

          // wait and go to next shard iterator
          Thread.sleep(intervalMillis)
          loop(getRecordsResult.getNextShardIterator)
        case Failure(t: Throwable) =>
          logger.error(s"failed get records, stream: $streamName")
          false
      }
    }


    Future {
      apiClient.getShardIterator(streamName, shardId, shardIteratorType) match {
        case Success(shardIterator) => loop(shardIterator)
        case Failure(t: Throwable) =>
          logger.error(s"failed get records, stream: $streamName")
          false
      }
    }
  }
}

object ApiConsumer extends LazyLogging {
  @throws(classOf[IllegalArgumentException])
  def apply(apiClient: ApiClient, streamName: String): ApiConsumer = {
    logger.debug(s"construct api consumer. stream name: $streamName")

    require(apiClient.checkStreamExistAndReady(streamName), s"The stream must be present and ready. name: $streamName")

    new ApiConsumer(apiClient, streamName)
  }

  @throws(classOf[IllegalArgumentException])
  def apply(streamName: String): ApiConsumer = this(ApiClient(), streamName)
}