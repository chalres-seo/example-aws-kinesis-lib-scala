package com.utils

import com.typesafe.scalalogging.LazyLogging

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