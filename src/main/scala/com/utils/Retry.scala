package com.utils

import com.typesafe.scalalogging.LazyLogging

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

trait Retry extends LazyLogging {
  private val defaultBackoffTimeInMillis: Long = AppConfig.DEFAULT_BACKOFF_TIME_IN_MILLIS

  def backoff(millis: Long): Unit = {
    logger.debug(s"back off $millis millis.")
    threadSleep(millis)
  }

  def backoff(): Unit = {
    logger.debug(s"back off $defaultBackoffTimeInMillis millis.")
    threadSleep(defaultBackoffTimeInMillis)
  }

  def threadSleep(): Unit = {
    this.threadSleep(defaultBackoffTimeInMillis)
  }

  def threadSleep(millis: Long): Unit = {
    try {
      Thread.sleep(millis)
    } catch {
      case e: InterruptedException => logger.error("interrupted sleep", e)
      case t: Throwable =>
        logger.error("unknown exception thread sleep")
        logger.error(t.getMessage, t)
    }
  }
}

object Retry extends LazyLogging with Retry {
  private val defaultBackoffTimeInMillis: Long = AppConfig.DEFAULT_BACKOFF_TIME_IN_MILLIS
  private val defaultMaxAttemptCount: Int = AppConfig.DEFAULT_ATTEMPT_COUNT

  def retry[T](fn: => T): T = this.retryWithBackoff(defaultMaxAttemptCount, defaultBackoffTimeInMillis)(fn)

  @throws(classOf[Exception])
  @tailrec
  def retryWithBackoff[T](attemptCount: Int, backoffMillis: Long)(fn: => T): T = {
    Try(fn) match {
      case Success(result) => result

      case Failure(e: Exception) if attemptCount > 0 =>
        logger.error(s"limit exceeded, retry with backoff. retry count remain: $attemptCount")
        logger.error(e.getMessage)

        this.backoff(backoffMillis)
        retryWithBackoff(attemptCount - 1, backoffMillis)(fn)

      case Failure(t: Throwable) =>
        if (attemptCount <= 0) logger.debug("attempts has been exceeded.")
        else logger.error(s"unknown exception.")
        logger.error(t.getMessage, t)
        throw t
    }
  }

}