package com.utils

import java.io.File

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.model.ShardIteratorType
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration
import com.typesafe.config.{Config, ConfigFactory}

object AppConfig {
  // read application.conf
  private val conf: Config = ConfigFactory.parseFile(new File("conf/application.conf")).resolve()

  // common config
  val DEFAULT_INTERVAL_MILLIS = conf.getLong("intervalMillis")

  // Retry config
  val DEFAULT_ATTEMPT_COUNT = conf.getInt("retry.attemptCount")
  val DEFAULT_BACKOFF_TIME_IN_MILLIS = conf.getLong("retry.backoffTimeInMillis")

  // aws account config
  val DEFAULT_AWS_PROFILE_NAME = conf.getString("aws.profile")
  val DEFAULT_AWS_REGION_NAME = conf.getString("aws.region")

  // kinesis config
  val DEFAULT_SHARD_COUNT = conf.getInt("aws.kinesis.shardCount")
  val DEFAULT_SHARD_ITERATOR_TYPE = ShardIteratorType.valueOf(conf.getString("aws.kinesis.shardIteratorType"))

  // kcl config
  val DEFAULT_CHECKPOINT_INTERVAL_MILLIS = conf.getLong("aws.kcl.checkPointIntervalMillis")
  val DEFAULT_KCL_INITIAL_STREAM_POSITION = InitialPositionInStream.valueOf(conf.getString("aws.kcl.initialStreamPosition"))

  // kpl config
  val DEFAULT_KPL_PROPS: KinesisProducerConfiguration = KinesisProducerConfiguration
    .fromPropertiesFile(conf.getString("aws.kpl.daemonPropsPath"))

}
