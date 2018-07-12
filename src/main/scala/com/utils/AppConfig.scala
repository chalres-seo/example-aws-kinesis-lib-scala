package com.utils

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.model.ShardIteratorType
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration

object AppConfig {
  // aws account config
  val DEFAULT_AWS_PROFILE_NAME = "default"
  val DEFAULT_AWS_REGION_NAME = "ap-northeast-2"

  // kinesis config
  val DEFAULT_SHARD_COUNT = 1
  val DEFAULT_INTERVAL_MILLIS = 1000L
  val DEFAULT_CHECKPOINT_INTERVAL_MILLIS = 60000L
  val DEFAULT_SHARD_ITERATOR_TYPE = ShardIteratorType.LATEST
  val DEFAULT_KCL_INITIAL_STREAM_POSITION = InitialPositionInStream.TRIM_HORIZON

  // Retry config
  val DEFAULT_ATTEMPT_COUNT = 10
  val DEFAULT_BACKOFF_TIME_IN_MILLIS = 1000L

  // kpl config
  val DEFAULT_KPL_PROPS: KinesisProducerConfiguration = KinesisProducerConfiguration
    .fromPropertiesFile("conf/aws_kinesis_kpl_default.properties")
}
