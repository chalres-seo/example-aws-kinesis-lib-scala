package com.aws.kinesis.api.consumer


import com.amazonaws.services.kinesis.model._
import com.aws.kinesis.api.ApiClient
import com.aws.kinesis.record.handler.ConsumeRecordsHandler
import com.typesafe.scalalogging.LazyLogging
import com.utils.AppConfig

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

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

  def consume(recordsHandler: ConsumeRecordsHandler.RecordsHandlerType): Vector[Future[Unit]] =
    consume(DEFAULT_INTERVAL_MILLIS, DEFAULT_SHARD_ITERATOR_TYPE)(recordsHandler)

  def consume(interValMillis: Long, shardIteratorType: ShardIteratorType)
             (recordsHandler: ConsumeRecordsHandler.RecordsHandlerType): Vector[Future[Unit]] = {
    apiClient.getShardList(streamName)
      .map(shard => {
        Future(consumeLoop(shard.getShardId, shardIteratorType, interValMillis)(recordsHandler))
      })
  }

  private def consumeLoop(shardId: String, shardIteratorType: ShardIteratorType, intervalMillis: Long)
                         (recordsHandler: ConsumeRecordsHandler.RecordsHandlerType): Unit = {
    logger.debug(s"start consume loop. loop-iterator-shardId-$shardId")

    val shardIterator = apiClient.getShardIterator(streamName, shardId, shardIteratorType)
    val getRecordsRequest: GetRecordsRequest = new GetRecordsRequest()

    @tailrec
    def loop(shardIterator: String): Unit = {
      logger.debug(s"consume shardIterator : $shardIterator")
      if (shardIterator == null) {
        logger.debug("next shard iterator is null. stop consuming")
      } else {
        getRecordsRequest.setShardIterator(shardIterator)
        val getRecordsResult: GetRecordsResult = apiClient.getRecords(getRecordsRequest)
        val records: Vector[Record] = getRecordsResult.getRecords.asScala.toVector

        if (records.nonEmpty) {
          recordsHandler(records)
        } else {
          logger.debug("consume records is null. next shardIterator")
        }

        Thread.sleep(intervalMillis)
        loop(getRecordsResult.getNextShardIterator)
      }
    }

    loop(shardIterator)
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