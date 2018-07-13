package com.aws.kinesis.library.consumer

import java.net.InetAddress
import java.util.UUID
import java.{lang, util}

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, KinesisClientLibConfiguration, Worker}
import com.aws.credentials.CredentialsFactory
import com.aws.kinesis.api.ApiClient
import com.aws.kinesis.library.consumer.processors.KclRecordsProcessorFactory
import com.aws.kinesis.record.handler.ConsumeRecordsHandler
import com.typesafe.scalalogging.LazyLogging
import com.utils.AppConfig

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Try

class KclConsumer(profileName: String,
                  regionName: String,
                  streamName: String,
                  consumeAppName: String,
                  initialStreamPosition: InitialPositionInStream
                 )(handler: ConsumeRecordsHandler.RecordsHandlerType) extends LazyLogging {

  private val workerId: String = Try(InetAddress.getLocalHost.getCanonicalHostName).getOrElse("unknown-host") + ":" + UUID.randomUUID()
  private val config: KinesisClientLibConfiguration = new KinesisClientLibConfiguration(consumeAppName,
      streamName,
      CredentialsFactory.getCredentialsProvider(profileName),
      workerId)
      .withRegionName(regionName)
      .withInitialPositionInStream(initialStreamPosition)

  private val worker: Worker = new Worker.Builder()
    .recordProcessorFactory(new KclRecordsProcessorFactory(handler))
    .config(config)
    .build()

  def consumeRecords(): Future[Unit] = {
    Future(this.worker.run())
  }

  def startGracefulShutdown(): util.concurrent.Future[lang.Boolean] = {
    this.worker.startGracefulShutdown()
  }
}

object KclConsumer extends LazyLogging {
  private val DEFAULT_AWS_PROFILE_NAME: String = AppConfig.DEFAULT_AWS_PROFILE_NAME
  private val DEFAULT_AWS_REGION_NAME: String = AppConfig.DEFAULT_AWS_REGION_NAME
  private val DEFAULT_KCL_INITIAL_STREAM_POSITION: InitialPositionInStream = AppConfig.DEFAULT_KCL_INITIAL_STREAM_POSITION

  def apply(profileName: String,
            regionName: String,
            streamName: String,
            consumeAppName: String,
            initialStreamPosition: InitialPositionInStream)
           (handler: ConsumeRecordsHandler.RecordsHandlerType): KclConsumer = {

    logger.debug(s"construct kcl consumer. profile: $profileName, region: $regionName, stream name: $streamName " +
      s"consume-app name: $consumeAppName, init-position: $initialStreamPosition")

    require(ApiClient(profileName, regionName).checkStreamExistAndReady(streamName),
      s"The stream must be present and ready. name: $streamName")

    new KclConsumer(profileName: String,
      regionName: String,
      streamName: String,
      consumeAppName: String,
      initialStreamPosition)(handler)
  }

  def apply(profileName: String,
            regionName: String,
            streamName: String,
            consumeAppName: String)
           (handler: ConsumeRecordsHandler.RecordsHandlerType): KclConsumer = {
    this(profileName: String,
      regionName: String,
      streamName: String,
      consumeAppName: String,
      DEFAULT_KCL_INITIAL_STREAM_POSITION)(handler)
  }

  def apply(streamName: String,
            consumeAppName: String,
            initialStreamPosition: InitialPositionInStream)
           (handler: ConsumeRecordsHandler.RecordsHandlerType): KclConsumer = {
    this(DEFAULT_AWS_PROFILE_NAME: String,
      DEFAULT_AWS_REGION_NAME: String,
      streamName: String,
      consumeAppName: String,
      initialStreamPosition)(handler)
  }

  def apply(streamName: String,
            consumeAppName: String)
           (handler: ConsumeRecordsHandler.RecordsHandlerType): KclConsumer = {
    this(DEFAULT_AWS_PROFILE_NAME: String,
      DEFAULT_AWS_REGION_NAME: String,
      streamName: String,
      consumeAppName: String,
      DEFAULT_KCL_INITIAL_STREAM_POSITION)(handler)
  }
}