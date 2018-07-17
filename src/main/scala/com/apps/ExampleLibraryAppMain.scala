package com.apps

import java.nio.file.{Files, Paths, StandardOpenOption}

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.model.{ResourceInUseException, ResourceNotFoundException}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.model.Record
import com.aws.kinesis.api.ApiClient
import com.aws.kinesis.library.consumer.KclConsumer
import com.aws.kinesis.library.producer.KplProducer
import com.aws.kinesis.record.StringRecord
import com.aws.kinesis.record.handler.{ConsumeRecordsHandler, RecordsHandler}
import com.typesafe.scalalogging.LazyLogging
import com.utils.AppConfig

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success, Try}

object ExampleLibraryAppMain extends LazyLogging {
  // default kinesis api client
  private val kinesisApiClient = ApiClient()

  // Your stream name.
  private val streamName: String = "test-stream"
  private val streamConsumerAppName = streamName + "-consumer-app"

  // Your stream shard count when create stream.
  private val yourShardCount:Int = 1

  // Your file location with records to write to the stream.
  private val yourProduceFromFilePathString = "example/example_record.txt"

  // Your file location where stream records will be written
  private val yourConsumeFileoutPathString = "example/example_record_kcl_consume.txt"

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

  def exampleRun(): Unit = {
    val kclConsumer: KclConsumer = this.getExampleConsumer(streamName, streamConsumerAppName, yourConsumeFileoutPathString)
    val kplProducer: KplProducer = KplProducer(streamName)

    val exampleRecords = Files.newBufferedReader(Paths.get(yourProduceFromFilePathString))
      .lines()
      .iterator()
      .asScala
      .map(line => StringRecord(s"pk-$line", line))
      .toVector

    kclConsumer.consumeRecords()

    // wait consumer bootstrap.
    Thread.sleep(40000)

    kplProducer.produceRecords(exampleRecords)

    // wait consumer consume example.
    Thread.sleep(10000)

    kclConsumer.startGracefulShutdown()
  }

  def getExampleConsumer(streamName: String, streamConsumerAppName: String, consumeRecordFileoutPathString: String): KclConsumer = {
    // composed records handler
    val recordsHandler: ConsumeRecordsHandler.KinesisRecordsHandlerType = {
      records:Vector[Record] => {
        val stringRecords: Vector[StringRecord] = StringRecord.recordsToStringRecords(records)

        Future(RecordsHandler.printStdout(stringRecords))
        Future(RecordsHandler.debugStdout(stringRecords))
        Future(RecordsHandler.tmpFileout(stringRecords, yourConsumeFileoutPathString, StandardOpenOption.APPEND, StandardOpenOption.CREATE))
      }
    }

    KclConsumer(streamName, streamConsumerAppName, InitialPositionInStream.LATEST)(recordsHandler)
  }

  def cleanUpResource(): Unit = {
    //Files.deleteIfExists(Paths.get(yourConsumeFileoutPathString))
    kinesisApiClient.deleteStream(streamName)
    kinesisApiClient.waitStreamDelete(streamName)

    val dynamodbClient = AmazonDynamoDBClientBuilder.standard()
      .withRegion(AppConfig.DEFAULT_AWS_REGION_NAME)
      .build()

    if (dynamodbClient.listTables().getTableNames.contains(streamConsumerAppName)) {
      Try(dynamodbClient.deleteTable(streamConsumerAppName)) match {
        case Success(_) =>
          while(!dynamodbClient.listTables().getTableNames.contains(streamConsumerAppName)) {
            Thread.sleep(3000)
          }
        case Failure(_: ResourceInUseException) => Unit
        case Failure(_: ResourceNotFoundException) => Unit
        case Failure(_) => Unit
      }
    }
  }
}
