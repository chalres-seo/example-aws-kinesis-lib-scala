package com.aws.kinesis.kcl.processors


import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason
import com.amazonaws.services.kinesis.clientlibrary.types.{InitializationInput, ProcessRecordsInput, ShutdownInput}
import com.amazonaws.services.kinesis.model.Record
import com.typesafe.scalalogging.LazyLogging
import com.utils.{AppConfig, KinesisRetry}

import scala.collection.JavaConverters._
import scala.collection.concurrent

trait IKclRecordsProcessor extends LazyLogging with IRecordProcessor {
  private val CHECKPOINT_INTERVAL_MILLIS: Long = AppConfig.DEFAULT_CHECKPOINT_INTERVAL_MILLIS

  private val status: concurrent.Map[String, AnyVal] = new java.util.concurrent.ConcurrentHashMap[String, AnyVal]().asScala

  override def initialize(initializationInput: InitializationInput): Unit = {
    val initShardId = initializationInput.getShardId
    logger.debug(s"initializing record processor[${this.getClass.getCanonicalName}] for shard: $initShardId")

    status.putIfAbsent("initShardId", initShardId.asInstanceOf[AnyVal])
  }

  override def processRecords(processRecordsInput: ProcessRecordsInput): Unit = {
    val records: Vector[Record] = processRecordsInput.getRecords.asScala.toVector
    val checkpointer: IRecordProcessorCheckpointer = processRecordsInput.getCheckpointer

    this.processRecordsWithRetries(records)

    if (System.currentTimeMillis() > status.getOrElseUpdate("nextCheckpointTimeInMillis",
      System.currentTimeMillis().asInstanceOf[AnyVal]).asInstanceOf[Long]) {
      checkpoint(checkpointer)
      status.putIfAbsent("nextCheckpointTimeInMillis", System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS)
    }
  }

  override def shutdown(shutdownInput: ShutdownInput): Unit = {
    val checkpointer = shutdownInput.getCheckpointer
    val shutdownReason = shutdownInput.getShutdownReason

    logger.debug(s"shutting down record processor[${this.getClass.getCanonicalName}] " +
      s"for shard: ${status("initShardId").asInstanceOf[String]}, reason: $shutdownReason")

    if (shutdownReason == ShutdownReason.TERMINATE) {
      checkpoint(checkpointer)
    }
  }

  private def checkpoint(checkpointer: IRecordProcessorCheckpointer): Unit = {
    logger.debug(s"checkpointing shard: ${status("initShardId").asInstanceOf[String]}")
    KinesisRetry.checkPointRetry(checkpointer.checkpoint())
  }

  def processRecordsWithRetries(records: Vector[Record]): Unit
}