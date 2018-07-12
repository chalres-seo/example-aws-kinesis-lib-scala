package com.aws.kinesis.api

import java.util.concurrent.TimeUnit

import com.amazonaws.services.kinesis.model._
import com.amazonaws.services.kinesis.{AmazonKinesisAsync, AmazonKinesisAsyncClientBuilder}
import com.aws.credentials.CredentialsFactory
import com.typesafe.scalalogging.LazyLogging
import com.utils.{AppConfig, KinesisRetry}

import scala.annotation.tailrec
import scala.collection.concurrent
import scala.collection.JavaConverters._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration


/**
  * Management Kinesis stream.
  *
  * @param awsProfileName target aws profile, default profile name is 'default'.
  * @param awsRegionName target aws region, default region name is 'ap-northeast-2'.
  * @param kinesisClient aws sdk kinesis client.
  */
class ApiClient(awsProfileName: String, awsRegionName: String, kinesisClient: AmazonKinesisAsync) extends LazyLogging {
  private val DEFAULT_SHARD_COUNT: Int = AppConfig.DEFAULT_SHARD_COUNT
  private val DEFAULT_INTERVAL_MILLIS: Long = AppConfig.DEFAULT_INTERVAL_MILLIS

  def createStream(streamName: String, shardCount: Int): Boolean = {
    logger.debug(s"create stream. name : $streamName, count : $shardCount")

    Try(KinesisRetry.apiRetry(kinesisClient.createStream(streamName, shardCount))) match {
      case Success(_) => true
      case Failure(_: ResourceInUseException) =>
        logger.debug(s"stream is already exist. name : $streamName")
        true
      case Failure(unknown) =>
        logger.error(s"unknown exception create stream. name : $streamName")
        logger.error(unknown.getMessage, unknown)
        false
    }
  }

  def createStream(streamName: String): Boolean = {
    this.createStream(streamName, DEFAULT_SHARD_COUNT)
  }

  def createStreamAndWaitReady(streamName: String): Boolean = {
    this.createStream(streamName)
    Await.result(this.watchStreamReady(streamName), Duration.Inf)
  }

  def deleteStream(streamName: String): Boolean = {
    logger.debug(s"delete stream. name : $streamName")

    Try(KinesisRetry.apiRetry(kinesisClient.deleteStream(streamName))) match {
      case Success(_) => true
      case Failure(_:ResourceNotFoundException) =>
        logger.debug(s"stream is not exist. name : $streamName")
        true
      case Failure(unknown) =>
        logger.error(s"unknown exception delete stream. name : $streamName")
        logger.error(unknown.getMessage, unknown)
        false
    }
  }

  /**
    *
    * @param streamName unchecked stream name.
    * @throws LimitExceededException retry and throw [[KinesisRetry]].
    * @throws ResourceNotFoundException throw.
    * @return stream description.
    */
  @throws(classOf[LimitExceededException])
  @throws(classOf[ResourceNotFoundException])
  private def getStreamDesc(streamName: String): StreamDescription = {
    logger.debug(s"get stream description. name : $streamName")

    KinesisRetry.apiRetry(kinesisClient.describeStream(streamName).getStreamDescription)
  }

  private def getStreamDesc(streamName: String, exclusiveStartShardId: String): StreamDescription = {
    logger.debug(s"get stream description exclusive start shardId. name : $streamName, shardId : $exclusiveStartShardId")

    KinesisRetry.apiRetry(kinesisClient.describeStream(streamName, exclusiveStartShardId).getStreamDescription)
  }

  /**
    * @param streamName unchecked stream name.
    * @return stream status
    *         CREATING, DELETING, ACTIVE, UPDATING [[StreamStatus]]
    *         custom : NOT_EXIST, UNKNOWN
    */
  def getStreamStatus(streamName: String): String = {
    logger.debug(s"get stream status. name : $streamName")

    Try(this.getStreamDesc(streamName)) match {
      case Success(streamDescription) => streamDescription.getStreamStatus
      case Failure(_: ResourceNotFoundException) =>
        logger.debug("resource not found. (check stream status. not error)")
        "NOT_EXIST"
      case Failure(t) =>
        logger.error(t.getMessage)
        logger.error("unknown exception", t)
        "UNKNOWN"
    }
  }


  def isStreamExist(streamName: String): Boolean = {
    logger.debug(s"check stream exists. name : $streamName")

    this.getStreamStatus(streamName) != "NOT_EXIST"
  }

  def isStreamReady(streamName: String): Boolean = {
    logger.debug(s"check stream ready. name : $streamName")

    this.getStreamStatus(streamName) == "ACTIVE"
  }

  def watchStreamReady(streamName: String, intervalMillis: Long): Future[Boolean] = {
    logger.debug(s"watch stream ready. name : $streamName")

    @tailrec
    def loop(streamStatus: String): Boolean = {
      streamStatus match {
        case "ACTIVE" =>
          logger.debug(s"stream status is $streamStatus. name : $streamName.")
          true
        case s if s == "CREATING" || s == "UPDATING" =>
          logger.debug(s"stream status is $streamStatus, wait for $intervalMillis millis stream is ready. name :$streamName")
          KinesisRetry.backoff(intervalMillis)
          loop(this.getStreamStatus(streamName))
        case s if s == "DELETING" || s == "NOT_EXIST" =>
          logger.error(s"stream status is $streamStatus, can't watch stream ready. name : $streamName")
          false
      }
    }
    Future(loop(this.getStreamStatus(streamName)))
  }

  def watchStreamReady(streamName: String): Future[Boolean] = {
    this.watchStreamReady(streamName, DEFAULT_INTERVAL_MILLIS)
  }

  def waitStreamReady(streamName: String, intervalMillis: Long, waitTimeMillis: Long): Boolean = {
    Await.result(this.watchStreamReady(streamName, intervalMillis), Duration.apply(waitTimeMillis, TimeUnit.MILLISECONDS))
  }

  def waitStreamReady(streamName: String): Boolean = {
    Await.result(this.watchStreamReady(streamName), Duration.Inf)
  }

  def watchStreamDelete(streamName: String, intervalMillis: Long): Future[Boolean] = {
    logger.debug(s"watch stream delete. name : $streamName, interval : $intervalMillis millis")

    @tailrec
    def loop(streamStatus: String): Boolean = {
      streamStatus match {
        case "NOT_EXIST" =>
          logger.debug(s"stream status is $streamStatus. name : $streamName.")
          true
        case "DELETING" =>
          logger.debug(s"stream status is $streamStatus, wait for $intervalMillis millis stream is delete. name :$streamName")
          KinesisRetry.backoff(intervalMillis)
          loop(this.getStreamStatus(streamName))
        case s if s == "ACTIVE" || s == "CREATING" || s == "UPDATING" =>
          logger.error(s"stream status is $streamStatus, can't watch stream delete. name : $streamName")
          false
      }
    }
    Future(loop(this.getStreamStatus(streamName)))
  }

  def watchStreamDelete(streamName: String): Future[Boolean] = {
    this.watchStreamDelete(streamName, DEFAULT_INTERVAL_MILLIS)
  }

  def waitStreamDelete(streamName: String, intervalMillis: Long, waitTimeMillis: Long): Boolean = {
    Await.result(this.watchStreamDelete(streamName, intervalMillis), Duration.apply(waitTimeMillis, TimeUnit.MILLISECONDS))
  }

  def waitStreamDelete(streamName: String): Boolean = {
    Await.result(this.watchStreamDelete(streamName), Duration.Inf)
  }

  /**
    *
    * @throws LimitExceededException retry and throw [[KinesisRetry]].
    * @return stream name list.
    */
  @throws(classOf[LimitExceededException])
  def getStreamList: Vector[String] = {
    logger.debug("get stream list.")

    @tailrec
    def loop(currentListStreamResult: ListStreamsResult, currrentStreamNames: Vector[String]): Vector[String] = {
      (currentListStreamResult.isHasMoreStreams.booleanValue(), currrentStreamNames.nonEmpty) match {
        case (true, true) =>
          logger.debug("list stream result has more result.")
          val nextListStreamResult: ListStreamsResult = KinesisRetry.apiRetry(kinesisClient.listStreams(currrentStreamNames.last))
          loop(nextListStreamResult, currrentStreamNames ++ nextListStreamResult.getStreamNames.asScala.toVector)
        case (true, false) =>
          val nextListStreamResult: ListStreamsResult = KinesisRetry.apiRetry(kinesisClient.listStreams)
          loop(nextListStreamResult, nextListStreamResult.getStreamNames.asScala.toVector)
        case (false, _) => currrentStreamNames
      }
    }

    val listStreamResult: ListStreamsResult = KinesisRetry.apiRetry(kinesisClient.listStreams())
    loop(listStreamResult, listStreamResult.getStreamNames.asScala.toVector)
  }

  /**
    *
    * @param streamName unchecked stream name.
    * @throws LimitExceededException retry and throw [[KinesisRetry]].
    * @throws ResourceNotFoundException throw.
    * @return shard list.
    */
  @throws(classOf[LimitExceededException])
  @throws(classOf[ResourceNotFoundException])
  def getShardList(streamName: String): Vector[Shard] = {
    logger.debug(s"get shard list. name : $streamName")

    @tailrec
    def loop(currentStreamDescription: StreamDescription, currentShardList: Vector[Shard]): Vector[Shard] = {
      (currentStreamDescription.isHasMoreShards.booleanValue(), currentShardList.nonEmpty) match {
        case (false, _) => currentShardList
        case (true, true) =>
          val nextStreamDescription = this.getStreamDesc(streamName, currentShardList.last.getShardId)
          loop(nextStreamDescription, currentShardList ++ nextStreamDescription.getShards.asScala.toVector)
        case (true, false) =>
          val nextStreamDescription = this.getStreamDesc(streamName)
          loop(nextStreamDescription, nextStreamDescription.getShards.asScala.toVector)
      }
    }

    val streamDescription = this.getStreamDesc(streamName)
    loop(streamDescription, streamDescription.getShards.asScala.toVector)
  }

  /**
    *
    * @param streamName unchecked stream name.
    * @param shardId shardIterator of shardId.
    * @param shardIteratorType shardIterator type. [[ShardIteratorType]].
    *
    * @throws ResourceNotFoundException stream is not exist. [[kinesisClient.getShardIterator]]
    * @throws InvalidArgumentException shardId or shardIterator type error. [[kinesisClient.getShardIterator]]
    * @throws ProvisionedThroughputExceededException available throughput error. [[kinesisClient.getShardIterator]]
    * @return single shard iterator.
    */
  @throws(classOf[ResourceNotFoundException])
  @throws(classOf[InvalidArgumentException])
  @throws(classOf[ProvisionedThroughputExceededException])
  def getShardIterator(streamName: String, shardId: String, shardIteratorType: ShardIteratorType): String = {
    logger.debug(s"get shard iterator. name: $streamName, shardId: $shardId, type: $shardIteratorType")

    kinesisClient.getShardIterator(streamName, shardId, shardIteratorType.toString).getShardIterator
  }

  /**
    *
    * @param streamName unchecked stream name.
    * @param shardIteratorType shardIterator type. [[ShardIteratorType]].
    *
    * @see [[getShardIterator]]
    *
    * @return shard iterator list. exclusive shard which failed get shard iterator.
    */
  def getShardIteratorList(streamName: String, shardIteratorType: ShardIteratorType): Vector[String] = {
    logger.debug(s"get shard iterator list. name: $streamName, type: $shardIteratorType")

    val getShardIteratorFutures: Vector[Future[Try[String]]] = Try(this.getShardList(streamName)) match {
      case Success(shardList) => shardList.map(shard => {
          logger.debug(s"get shard iterator parallel. shardId: ${shard.getShardId}")
          Future {
            Try(this.getShardIterator(streamName, shard.getShardId, shardIteratorType)).recoverWith {
              case throwable =>
                logger.debug(s"failed. get shard iterator. skipped shard: ${shard.getShardId}")
                Failure(throwable)
            }
          }
        })
      case Failure(throwable) =>
        logger.error("failed get shard list.")
        throw throwable
    }

    logger.debug(s"await result get shard iterator. name: $streamName, type: $shardIteratorType")

    for {
      future <- getShardIteratorFutures
      if Await.result(future, Duration.Inf).isSuccess
    } yield Await.result(future, Duration.Inf).get
  }

  /**
    *
    * @param putRecordsRequest provided request.
    * @throws ResourceNotFoundException put records exception. [[kinesisClient.putRecords]]
    * @throws InvalidArgumentException put records exception. [[kinesisClient.putRecords]]
    * @throws ProvisionedThroughputExceededException put records exception. [[kinesisClient.putRecords]]
    * @throws KMSDisabledException put records exception. [[kinesisClient.putRecords]]
    * @throws KMSInvalidStateException put records exception. [[kinesisClient.putRecords]]
    * @throws KMSAccessDeniedException put records exception. [[kinesisClient.putRecords]]
    * @throws KMSNotFoundException put records exception. [[kinesisClient.putRecords]]
    * @throws KMSOptInRequiredException put records exception. [[kinesisClient.putRecords]]
    * @throws KMSThrottlingException put records exception. [[kinesisClient.putRecords]]
    * @return put record result.
    */
  @throws(classOf[Exception])
  def putRecords(putRecordsRequest: PutRecordsRequest): PutRecordsResult = {
    logger.debug(s"put records request. stream name: ${putRecordsRequest.getStreamName}, count: ${putRecordsRequest.getRecords.size()}")
    kinesisClient.putRecords(putRecordsRequest)
  }

  /**
    *
    * @param getRecordsRequest provided request.
    * @throws ResourceNotFoundException put records exception. [[kinesisClient.putRecords]]
    * @throws InvalidArgumentException put records exception. [[kinesisClient.putRecords]]
    * @throws ProvisionedThroughputExceededException put records exception. [[kinesisClient.putRecords]]
    * @throws KMSDisabledException put records exception. [[kinesisClient.putRecords]]
    * @throws KMSInvalidStateException put records exception. [[kinesisClient.putRecords]]
    * @throws KMSAccessDeniedException put records exception. [[kinesisClient.putRecords]]
    * @throws KMSNotFoundException put records exception. [[kinesisClient.putRecords]]
    * @throws KMSOptInRequiredException put records exception. [[kinesisClient.putRecords]]
    * @throws KMSThrottlingException put records exception. [[kinesisClient.putRecords]]
    * @return get record result.
    */
  @throws(classOf[Exception])
  def getRecords(getRecordsRequest: GetRecordsRequest): GetRecordsResult = {
    logger.debug(s"get records request.")
    kinesisClient.getRecords(getRecordsRequest)
  }

  /**
    *
    * @param streamName unchecked stream name.
    * @throws ResourceNotFoundException throw exception when stream is not exist or ready.
    * @return returns true if the stream is ready. Otherwise, it returns false.
    */
  @throws(classOf[ResourceNotFoundException])
  def checkStreamExistAndReady(streamName: String): Boolean = {
    if (!this.isStreamExist(streamName)) {
      logger.error(s"stream is not exist. name: $streamName")
      //throw new ResourceNotFoundException(s"stream is not exist. name: $streamName")
      return false
    }

    if (!this.isStreamReady(streamName)) {
      logger.error(s"stream is not ready. wait for ready, name: $streamName")
      if (!KinesisRetry.apiRetry(this.waitStreamReady(streamName))) {
        logger.error(s"failed wait for stream ready. name: $streamName")
        //throw new ResourceNotFoundException(s"stream is exist but not ready. name: $streamName")
        return false
      }
    }

    this.isStreamReady(streamName)
  }
}

object ApiClient extends LazyLogging {
  private val DEFAULT_AWS_PROFILE_NAME = AppConfig.DEFAULT_AWS_PROFILE_NAME
  private val DEFAULT_AWS_REGION_NAME = AppConfig.DEFAULT_AWS_REGION_NAME

  private val kinesisAsyncClientList: concurrent.Map[String, AmazonKinesisAsync] =
    new java.util.concurrent.ConcurrentHashMap[String, AmazonKinesisAsync]().asScala

  def apply(profileName: String, regionName: String): ApiClient = {
    logger.debug(s"construct api client. profileName: $profileName, regionName: $regionName")
    new ApiClient(profileName,
      regionName,
      this.getKinesisClient(profileName, regionName))
  }

  def apply(profileName: String): ApiClient = this(profileName, DEFAULT_AWS_REGION_NAME)
  def apply(): ApiClient = this(DEFAULT_AWS_PROFILE_NAME, DEFAULT_AWS_REGION_NAME)

  private def getKinesisClient(profileName: String, regionName: String): AmazonKinesisAsync = {
    logger.debug("get kinesis async client")
    logger.debug(s"profile : $profileName, region : $regionName")

    kinesisAsyncClientList
      .getOrElse(makeKey(profileName, regionName), this.createKinesisClient(profileName, regionName))
  }

  private def createKinesisClient(profileName: String, regionName: String): AmazonKinesisAsync = {
    logger.debug("create kinesis async client")
    logger.debug(s"profile : $profileName, region : $regionName")

    AmazonKinesisAsyncClientBuilder
      .standard()
      .withCredentials(CredentialsFactory.getCredentialsProvider(profileName))
      .withRegion(regionName)
      .build()
  }

  private def makeKey: String = this.makeKey(DEFAULT_AWS_REGION_NAME, DEFAULT_AWS_REGION_NAME)
  private def makeKey(profileName: String, regionName: String): String = s"$profileName::$regionName"
}