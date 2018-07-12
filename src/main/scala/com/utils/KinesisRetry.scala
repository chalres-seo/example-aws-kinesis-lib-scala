package com.utils

import com.amazonaws.services.kinesis.clientlibrary.exceptions.{InvalidStateException, ShutdownException, ThrottlingException}
import com.amazonaws.services.kinesis.model._
import com.typesafe.scalalogging.LazyLogging

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

object KinesisRetry extends LazyLogging with Retry {
  private val defaultBackoffTimeInMillis: Long = AppConfig.DEFAULT_BACKOFF_TIME_IN_MILLIS
  private val defaultMaxAttemptCount: Int = AppConfig.DEFAULT_ATTEMPT_COUNT

  def apiRetry[T](fn: => T): T = this.apiRetryWithBackoff(defaultMaxAttemptCount, defaultBackoffTimeInMillis)(fn)
  def checkPointRetry[T](fn: => T): Unit = this.checkPointRetryWithBackoff(defaultMaxAttemptCount, defaultBackoffTimeInMillis)(fn)
  def processRecordsRetry[T](fn: => T): Unit = this.processRecordsRetryWithBackoff(defaultMaxAttemptCount, defaultBackoffTimeInMillis)(fn)

  @throws(classOf[Exception])
  @tailrec
  def apiRetryWithBackoff[T](attemptCount: Int, backoffMillis: Long)(fn: => T): T = {
    Try(fn) match {
      case Success(result) => result

      case Failure(e: ResourceNotFoundException) =>
        logger.error(s"resource not found exception. the remaining attempts will be skipped.")
        logger.error(e.getMessage)
        throw e

      case Failure(e: ResourceInUseException) =>
        logger.error(s"resource in use exception. the remaining attempts will be skipped.")
        logger.error(e.getMessage)
        throw e

      case Failure(e: LimitExceededException) if attemptCount > 0 =>
        logger.error(s"limit exceeded, retry with backoff. retry count remain: $attemptCount")
        logger.error(e.getMessage)

        this.backoff(backoffMillis)
        apiRetryWithBackoff(attemptCount - 1, backoffMillis)(fn)

      case Failure(t: Throwable) =>
        if (attemptCount <= 0) logger.debug("attempts has been exceeded.")
        else logger.error(s"unknown exception.")
        logger.error(t.getMessage, t)
        throw t
    }
  }

  @tailrec
  def checkPointRetryWithBackoff[T](attemptCount: Int, backoffMillis: Long)(fn: => Unit): Unit = {
    Try(fn) match {
      case Success(_) =>
        logger.debug("success checkpoint")

      case Failure(e: LimitExceededException) if attemptCount > 0 =>
        logger.error(s"failed check point limit exceeded, retry with backoff. " +
          s"retry ${defaultMaxAttemptCount - attemptCount} of $defaultMaxAttemptCount")
        logger.error(e.getMessage)

        this.backoff(backoffMillis)
        checkPointRetryWithBackoff(attemptCount - 1, backoffMillis)(fn)

      case Failure(e: ThrottlingException) if attemptCount > 0 =>
        logger.error(s"failed checkpoint transient issue. " +
          s"retry ${defaultMaxAttemptCount - attemptCount} of $defaultMaxAttemptCount")
        logger.error(e.getMessage)

        this.backoff(backoffMillis)
        checkPointRetryWithBackoff(attemptCount - 1, backoffMillis)(fn)

      case Failure(e: ShutdownException) =>
        logger.debug("caught shutdown exception, skipping checkpoint.")
        logger.debug(e.getMessage)

      case Failure(e: InvalidStateException) =>
        logger.error("cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.")
        logger.error(e.getMessage)

      case Failure(t: Throwable) =>
        if (attemptCount <= 0) logger.debug("attempts has been exceeded.")
        else logger.error(s"unknown exception.")
        logger.error(t.getMessage, t)
    }
  }

  def processRecordsRetryWithBackoff[T](attemptCount: Int, backoffMillis: Long)(fn: => Unit): Unit = {
    Try(fn) match {
      case Success(_) =>
        logger.debug("success process records")

      case Failure(e: LimitExceededException) if attemptCount > 0 =>
        logger.error(s"failed check point limit exceeded, retry with backoff. " +
          s"retry ${defaultMaxAttemptCount - attemptCount} of $defaultMaxAttemptCount")
        logger.error(e.getMessage)

        this.backoff(backoffMillis)
        checkPointRetryWithBackoff(attemptCount - 1, backoffMillis)(fn)

      case Failure(e: ThrottlingException) if attemptCount > 0 =>
        logger.error(s"failed checkpoint transient issue. " +
          s"retry ${defaultMaxAttemptCount - attemptCount} of $defaultMaxAttemptCount")
        logger.error(e.getMessage)

        this.backoff(backoffMillis)
        checkPointRetryWithBackoff(attemptCount - 1, backoffMillis)(fn)

      case Failure(e: ShutdownException) =>
        logger.debug("caught shutdown exception, skipping checkpoint.")
        logger.debug(e.getMessage)

      case Failure(e: InvalidStateException) =>
        logger.error("cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.")
        logger.error(e.getMessage)

      case Failure(t: Throwable) =>
        if (attemptCount <= 0) logger.debug("attempts has been exceeded.")
        else logger.error(s"unknown exception.")
        logger.error(t.getMessage, t)
    }
  }
}