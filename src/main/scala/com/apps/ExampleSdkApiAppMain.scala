package com.apps

import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.kinesis.model.Record
import com.aws.kinesis.api.ApiClient
import com.aws.kinesis.api.consumer.ApiConsumer
import com.aws.kinesis.api.producer.ApiProducer
import com.aws.kinesis.record.StringRecord
import com.aws.kinesis.record.handler.{ConsumeRecordsHandler, RecordsHandler}
import com.typesafe.scalalogging.LazyLogging
import com.utils.AppConfig

import scala.collection.JavaConverters._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.Try

object ExampleSdkApiAppMain extends LazyLogging {
  // default kinesis api client
  private val kinesisApiClient = ApiClient()

  // Your stream name.
  private val streamName: String = "test-stream"

  // Your stream shard count when create stream.
  private val yourShardCount:Int = 1

  // Your file location with records to write to the stream.
  private val yourProduceFromFilePathString = "example/example_record.txt"

  // Your file location where stream records will be written
  private val yourConsumeFileoutPathString = "example/example_record_api_consume.txt"
  Files.deleteIfExists(Paths.get(yourConsumeFileoutPathString))

  def main(args: Array[String]): Unit = {
    // Check and create stream.
    if (!kinesisApiClient.isStreamExist(streamName)) {
      kinesisApiClient.createStream(streamName)
    }

    // Clean up write file
    Files.deleteIfExists(Paths.get(yourConsumeFileoutPathString))

    // Your Code here
    exampleRun()
    cleanUpResource()

  }
  // Your Code here
  def exampleRun(): Unit = {
    // consumer start.
    val consumerFutures = this.consume(streamName, yourConsumeFileoutPathString)

    // wait consumer bootstrap.
    Thread.sleep(5000)

    // produce example.
    this.produce(streamName, yourProduceFromFilePathString)

    // wait consumer consume example.
    logger.debug(Try(Await.result(consumerFutures.head, Duration(10, TimeUnit.SECONDS))).failed.get.getMessage)
  }

  def createStream(streamName: String, shardCount: Int): Boolean = {
    if (kinesisApiClient.createStream(streamName, shardCount)) {
      println(s"stream was created. name: $streamName")
    } else {
      println(s"stream already exist. name: $streamName")
    }

    kinesisApiClient.isStreamExist(streamName)
  }

  def createStreamAndWaitReady(streamName: String, shardCount: Int): Boolean = {
    this.createStream(streamName, shardCount)

    if (kinesisApiClient.waitStreamReady(streamName)) {
      println(s"stream is ready. name: $streamName")
    } else {
      println(s"failed. wait for stream ready. name: $streamName")
    }

    kinesisApiClient.isStreamReady(streamName)
  }

  def deleteStream(streamName: String): Boolean = {
    if (kinesisApiClient.deleteStream(streamName)) {
      println(s"stream is delete. name: $streamName")
    } else {
      println(s"stream is not exist. name: $streamName")
    }

    !kinesisApiClient.isStreamExist(streamName)
  }

  def produce(streamName: String, produceRecordFromFilePathString: String): Future[Boolean] = {
    val apiProducer = ApiProducer(streamName)
    val produceRecordFromFilePath = Paths.get(produceRecordFromFilePathString)

    val lines = Files.newBufferedReader(produceRecordFromFilePath).lines().iterator().asScala

    apiProducer.produce(lines.map(line => StringRecord(s"pk-$line", line)).toVector)
  }

  def consume(streamName: String, consumeRecordFileoutPathString: String): Vector[Future[Unit]] = {
    val apiConsumer: ApiConsumer = ApiConsumer(streamName)

    // composed records handler
    val recordsHandler: ConsumeRecordsHandler.RecordsHandlerType = {
      records:Vector[Record] => {
        Future(RecordsHandler.debugStdout(records))
        Future(RecordsHandler.debugData(records))
        Future(RecordsHandler.tmpFileout(consumeRecordFileoutPathString, append = false, records))
      }
    }

    apiConsumer.consume(recordsHandler)
  }

  def cleanUpResource(): Unit = {
    //Files.deleteIfExists(Paths.get(yourConsumeFileoutPathString))
    kinesisApiClient.deleteStream(streamName)
    kinesisApiClient.waitStreamDelete(streamName)
  }
}