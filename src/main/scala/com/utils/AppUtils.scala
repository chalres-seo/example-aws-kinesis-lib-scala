package com.utils

import java.nio.file.{Files, Paths}

import com.typesafe.scalalogging.LazyLogging

object AppUtils extends LazyLogging {
  def checkDirAndIfNotExistCreate(filePathString: String): Boolean = {
    logger.debug("check file path and create.")
    val path = Paths.get(filePathString)

    if (Files.notExists(path.getParent)) {
      logger.debug(s"dir is not exist. create: ${path.getParent}")
      Files.createDirectory(path.getParent)
      true
    } else {
      logger.debug("file path is exist.")
      false
    }
  }
}
