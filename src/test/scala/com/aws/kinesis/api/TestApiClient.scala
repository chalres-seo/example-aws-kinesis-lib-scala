package com.aws.kinesis.api


import java.nio.file.{Files, Paths, StandardOpenOption}
import java.util.concurrent.TimeUnit

import com.amazonaws.services.kinesis.model.{Record, Shard, ShardIteratorType}
import com.aws.kinesis.api.consumer.ApiConsumer
import com.aws.kinesis.api.producer.ApiProducer
import com.aws.kinesis.record.StringRecord
import com.aws.kinesis.record.handler.RecordsHandler
import org.hamcrest.CoreMatchers._
import com.typesafe.scalalogging.LazyLogging
import org.junit.{Assert, FixMethodOrder, Test}
import org.junit.runners.MethodSorters

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.util.Try
import scala.concurrent.ExecutionContext.Implicits.global

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
class TestApiClient extends LazyLogging {
  private val apiClient: ApiClient = ApiClient()

  private val testStreamName: String = "test-stream"

  private val tmpFilePathString = "tmp/file.out"

  private val testProduceRecordCount = 10

  private val waitSec = 5
  private val waitMillis = 5000L


  def setup(): Unit = {
    if (!apiClient.isStreamExist(testStreamName)) {
      apiClient.createStream(testStreamName)
      apiClient.waitStreamReady(testStreamName)
    }

    Files.deleteIfExists(Paths.get(tmpFilePathString))
  }

  def cleanUp(): Unit = {
    if (apiClient.isStreamExist(testStreamName)) {
      apiClient.deleteStream(testStreamName)
      apiClient.waitStreamDelete(testStreamName)
    }

    Files.deleteIfExists(Paths.get(tmpFilePathString))
  }

  @Test
  def test99ClenUpResource(): Unit = {
    cleanUp()
  }

  @Test
  def test01StreamIsExist(): Unit = {
    this.cleanUp()

    Assert.assertThat(apiClient.isStreamExist(testStreamName), is(false))
    Assert.assertThat(apiClient.getStreamList.get.contains(testStreamName), is(false))
  }

  @Test
  def test02StreamCreateAndWaitReady(): Unit = {
    this.cleanUp()

    Assert.assertThat(apiClient.isStreamReady(testStreamName), is(false))
    Assert.assertThat(apiClient.waitStreamReady(testStreamName), is(false))

    Assert.assertThat(apiClient.createStream(testStreamName), is(true))
    Assert.assertThat(apiClient.createStream(testStreamName), is(true))

    Assert.assertThat(apiClient.isStreamExist(testStreamName), is(true))
    Assert.assertThat(apiClient.isStreamReady(testStreamName), is(false))

    Assert.assertThat(apiClient.waitStreamReady(testStreamName), is(true))
    Assert.assertThat(apiClient.waitStreamReady(testStreamName), is(true))

    Assert.assertThat(apiClient.isStreamReady(testStreamName), is(true))
  }

  @Test
  def test03StreamList(): Unit = {
    this.setup()

    Assert.assertThat(apiClient.getStreamList.get.contains(testStreamName + "_error"), is(false))
    Assert.assertThat(apiClient.getStreamList.get.contains(testStreamName), is(true))
  }

  @Test
  def test04StreamDeleteAndWait(): Unit = {
    this.setup()

    Assert.assertThat(apiClient.waitStreamDelete(testStreamName), is(false))

    Assert.assertThat(apiClient.deleteStream(testStreamName), is(true))
    Assert.assertThat(apiClient.deleteStream(testStreamName), is(true))

    Assert.assertThat(apiClient.isStreamReady(testStreamName), is(false))
    Assert.assertThat(apiClient.isStreamExist(testStreamName), is(true))

    Assert.assertThat(apiClient.waitStreamDelete(testStreamName), is(true))
    Assert.assertThat(apiClient.waitStreamDelete(testStreamName), is(true))

    Assert.assertThat(apiClient.isStreamReady(testStreamName), is(false))
    Assert.assertThat(apiClient.isStreamExist(testStreamName), is(false))
  }

  @Test
  def test05GetShardList(): Unit = {
    this.setup()

    val shardList: Vector[Shard] = apiClient.getShardList(testStreamName).get
    Assert.assertThat(shardList.length, is(1))

    val shardIteratorList: Vector[String] = apiClient.getShardIteratorList(testStreamName, ShardIteratorType.LATEST).get
    Assert.assertThat(shardIteratorList.length, is(1))
  }

  @Test
  def test06APIConsumeAndProduce(): Unit = {
    this.setup()

    val apiConsumer: ApiConsumer = ApiConsumer(testStreamName)
    val apiProducer = ApiProducer(testStreamName)

    val consumerFutures: Try[Vector[Future[Boolean]]] = apiConsumer.consume {
      records:Vector[Record] => {
        val stringRecords: Vector[StringRecord] = StringRecord.recordsToStringRecords(records)

        Future(RecordsHandler.printStdout(stringRecords))
        Future(RecordsHandler.debugStdout(stringRecords))
        Future(RecordsHandler.tmpFileout(stringRecords, tmpFilePathString, StandardOpenOption.APPEND, StandardOpenOption.CREATE))
      }
    }

    Thread.sleep(waitMillis)
    apiProducer.produce(StringRecord.createExampleRecords(testProduceRecordCount))

    val awaitResult: Try[Unit] = Try(Await.result(consumerFutures.get.head, Duration(waitSec, TimeUnit.SECONDS)))

    Assert.assertThat(awaitResult.failed.get.getMessage, is(s"Futures timed out after [$waitSec seconds]"))
    Assert.assertThat(Files.newBufferedReader(Paths.get(tmpFilePathString)).lines().count().toInt, is(testProduceRecordCount))
  }

  @Test
  def testPutRecord():Unit = {
    val apiProducer = ApiProducer(testStreamName)

    apiProducer.produce(StringRecord.createExampleRecords(500))
    Thread.sleep(1000)
    Await.result(apiProducer.produce(StringRecord.createExampleRecords(500)), Duration.Inf)
  }
}