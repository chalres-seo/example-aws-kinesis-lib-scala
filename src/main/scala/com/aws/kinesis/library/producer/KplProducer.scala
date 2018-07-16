package com.aws.kinesis.library.producer

import com.amazonaws.services.kinesis.producer._
import com.aws.credentials.CredentialsFactory
import com.aws.kinesis.api.ApiClient
import com.aws.kinesis.record.RecordImpl
import com.google.common.util.concurrent.ListenableFuture
import com.typesafe.scalalogging.LazyLogging
import com.utils.AppConfig

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

case class KplProducer(profileName: String,
                  regionName: String,
                  streamName: String,
                  private val kinesisProducer: KinesisProducer) extends LazyLogging {

  def produceRecords[T](records: Vector[RecordImpl[T]]): Future[Boolean] = {
    logger.debug(s"producer records. stream name: $streamName, record count: ${records.size}")

    @tailrec
    def loop(currentFailedRecord: Vector[RecordImpl[T]]): Vector[RecordImpl[T]] = {
      if (currentFailedRecord.nonEmpty) {
        logger.debug(s"re-produce failed record. stream name: $streamName, record count: ${currentFailedRecord.size}")
        loop(this.addUserRecords(currentFailedRecord))
      } else {
        currentFailedRecord
      }
    }
    Future(loop(this.addUserRecords(records)).isEmpty)
  }

  /**
    *
    * @param records produce records.
    * @tparam T produce record data type.
    *
    * [[KinesisProducer].addUserRecord]]
    *
    * @throws java.lang.IllegalArgumentException kpl exception.
    * @throws com.amazonaws.services.kinesis.producer.DaemonException kpl exception.
    *
    * @return filed records.
    */
  @throws(classOf[IllegalArgumentException])
  @throws(classOf[DaemonException])
  private def addUserRecords[T](records: Vector[RecordImpl[T]]): Vector[RecordImpl[T]] = {
    this.getFailedRecords {
      for {
        record <- records
        if record.getByteBuffer.isSuccess
      } yield {
        logger.debug(s"put record. stream name: $streamName, record: $record")
        (record, kinesisProducer.addUserRecord(streamName, record.getPartitionKey, record.getByteBuffer.get))
      }
    }
  }

  private def getFailedRecords[T](addUserRecordResultFutures: Vector[(RecordImpl[T],
    ListenableFuture[UserRecordResult])]): Vector[RecordImpl[T]] = {
    for {
      result <- addUserRecordResultFutures
      if !result._2.get().isSuccessful
    } yield result._1
  }
}

object KplProducer extends LazyLogging {
  private val defaultProfileName = AppConfig.DEFAULT_AWS_PROFILE_NAME
  private val defaultRegionName = AppConfig.DEFAULT_AWS_REGION_NAME

  private val defaultKplConfigProps = AppConfig.DEFAULT_KPL_PROPS

  @throws(classOf[IllegalArgumentException])
  def apply(profileName: String, regionName:String, streamName: String): KplProducer = {
    logger.debug(s"construct kpl producer. profile: $profileName, region: $regionName, stream name: $streamName")

    require(ApiClient(profileName, regionName).checkStreamExistAndReady(streamName),
      s"The stream must be present and ready. name: $streamName")

    synchronized {
      defaultKplConfigProps
        .setRegion(regionName)
        .setCredentialsProvider(CredentialsFactory.getCredentialsProvider(profileName))

      new KplProducer(profileName, regionName, streamName, new KinesisProducer(defaultKplConfigProps))
    }
  }

  @throws(classOf[IllegalArgumentException])
  def apply(streamName: String): KplProducer = {
    this(defaultProfileName, defaultRegionName, streamName)
  }
}