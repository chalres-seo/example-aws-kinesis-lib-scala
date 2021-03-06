package com.aws.kinesis.kcl

import java.nio.file.{Files, Paths, StandardOpenOption}
import java.{lang, util}

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.model.{ResourceInUseException, ResourceNotFoundException}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.model.{Record, ShardIteratorType}
import com.aws.kinesis.api.ApiClient
import com.aws.kinesis.library.consumer.KclConsumer
import com.aws.kinesis.library.producer.KplProducer
import com.aws.kinesis.record.StringRecord
import com.aws.kinesis.record.handler.RecordsHandler
import org.hamcrest.CoreMatchers._
import com.typesafe.scalalogging.LazyLogging
import com.utils.AppConfig
import org.junit.{Assert, FixMethodOrder, Test}
import org.junit.runners.MethodSorters

import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
class TestKclConsumerAndKplProducer extends LazyLogging {
  private val apiClient: ApiClient = ApiClient()

  private val testStreamName: String = "test-stream"

  private val tmpFilePathString = "tmp/file.out"

  private val testConsumerAppName = "test-consumer-app"

  private val testProduceRecordCount = 10

  private val waitSec = 5
  private val waitMillis = 5000L

  def setup(): Unit = {
    if (!apiClient.isStreamExist(testStreamName)) {
      apiClient.createStream(testStreamName)
      apiClient.waitStreamReady(testStreamName)
    }

    Files.deleteIfExists(Paths.get(tmpFilePathString))

    val dynamodbClient = AmazonDynamoDBClientBuilder.standard()
      .withRegion(AppConfig.DEFAULT_AWS_REGION_NAME)
      .build()

    if (dynamodbClient.listTables().getTableNames.contains(testConsumerAppName)) {
      dynamodbClient.deleteTable(testConsumerAppName)

      while(!dynamodbClient.listTables().getTableNames.contains(testConsumerAppName)) {
        Thread.sleep(waitMillis)
      }
    }
  }

  def cleanUp(): Unit = {
    if (apiClient.isStreamExist(testStreamName)) {
      apiClient.deleteStream(testStreamName)
      apiClient.waitStreamDelete(testStreamName)
    }

    Files.deleteIfExists(Paths.get(tmpFilePathString))

    val dynamodbClient = AmazonDynamoDBClientBuilder.standard()
      .withRegion(AppConfig.DEFAULT_AWS_REGION_NAME)
      .build()

    if (dynamodbClient.listTables().getTableNames.contains(testConsumerAppName)) {
      Try(dynamodbClient.deleteTable(testConsumerAppName)) match {
        case Success(_) =>
          while(!dynamodbClient.listTables().getTableNames.contains(testConsumerAppName)) {
            Thread.sleep(waitMillis)
          }
        case Failure(_: ResourceInUseException) => Unit
        case Failure(_: ResourceNotFoundException) => Unit
        case Failure(_) => Unit
      }
    }
  }

  @Test
  def test99ClenUpResource(): Unit = {
    cleanUp()
  }

  @Test
  def test01KclConsumeAndKplProduce(): Unit = {
    this.setup()

    val kplProducer: KplProducer = KplProducer(testStreamName)
    val kclConsumer = KclConsumer(testStreamName, testConsumerAppName, InitialPositionInStream.LATEST) {
      records:Vector[Record] => {
        val stringRecords: Vector[StringRecord] = StringRecord.recordsToStringRecords(records)

        Future(RecordsHandler.printStdout(stringRecords))
        Future(RecordsHandler.debugStdout(stringRecords))
        Future(RecordsHandler.tmpFileout(stringRecords, tmpFilePathString, StandardOpenOption.APPEND, StandardOpenOption.CREATE))
      }
    }

    val kclConsumerFuture = kclConsumer.consumeRecords()

    Thread.sleep(waitMillis * 6L)

    kplProducer.produceRecords(StringRecord.createExampleRecords(testProduceRecordCount))

    Thread.sleep(waitMillis)


    val kclConsumerShutdownFuture: util.concurrent.Future[lang.Boolean] = kclConsumer.startGracefulShutdown()
    kclConsumerShutdownFuture.get()

    Assert.assertThat(Files.newBufferedReader(Paths.get(tmpFilePathString)).lines().count().toInt, is(testProduceRecordCount))
  }
}
