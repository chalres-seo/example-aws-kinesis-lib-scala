# aws-kinesis-lib-scala

Example Scala library for AWS Kinesis SDK, KCL, KPL

* This example does not cover advanced topics. (re-sharding, exactly once...)
* All consumers follow at least one processing rule.

## Futures

* AWS Kinesis SDK Stream Management ([SDK Doc] / [SDK Git])
* AWS Kinesis SDK Producer
* AWS Kinesis SDK Consumer
* AWS Kinesis Producer Library Producer ([KPL Doc] / [KPL Git])
* AWS Kinesis Client Library Consumer ([KCL Doc] / [KCL Git])

## Usage

#### SDK Api Client

* Constructor

```Scala
package com.aws.kinesis.api

object ApiClient {
  def apply(profileName: String, regionName: String)
  def apply(profileName: String)
  def apply()
}
```

* Create and delete stream
* Get stream list
* Check stream status

#### SDK Api Producer

* Constructor

```Scala
package com.aws.kinesis.api.producer

object ApiProducer {
  def apply(apiClient: ApiClient, streamName: String)
  def apply(streamName: String)
}
```

* Produce record interface

```Scala
trait RecordImpl[T] {
  def getPartitionKey: String
  def getData: T
  def getByteBuffer: Try[ByteBuffer]
  def getSequenceNumber: Option[String]
  def getKinesisRecord: Record
}
```

* Produce record

```Scala
def produce[T](records: Vector[RecordImpl[T]]): Future[Boolean]

produce(YOUR_PRODUCE_RECORDS)
```

#### SDK Api Consumer

* Constructor

```Scala
package com.aws.kinesis.api.consumer

object ApiConsumer {
  def apply(apiClient: ApiClient, streamName: String)
  def apply(streamName: String)
}
```

* Consume record

```Scala
type RecordsHandlerType = Vector[Record] => Unit

def consume(recordsHandler: RecordsHandlerType): Vector[Future[Unit]]

consume(yourRecordsHandler)
// or
consume {
  // Direct implement your records handler
  records:Vector[Record] => {
    // Your record handling code here.
  }
}

def consume(interValMillis: Long, shardIteratorType: ShardIteratorType)(recordsHandler: RecordsHandlerType): Vector[Future[Unit]]

consume(YOUR_INTERVEL_MILLIS, YOUR_SHARD_ITERATOR_TYPE)(YOUR_RECORDS_HANDLER)
// or
consume(YOUR_INTERVEL_MILLIS, YOUR_SHARD_ITERATOR_TYPE) {
  // Direct implement your records handler
  records:Vector[Record] => {
    // Your record handling code here.
  }
}
```

* Stop

Consumer stop when receive interrupt signal

#### KPL Producer

* Constructor

```Scala
object KplProducer {
  def apply(profileName: String, regionName:String, streamName: String)
  def apply(streamName: String): KplProducer
```

* Produce record

```Scala
def produceRecords[T](records: Vector[RecordImpl[T]]): Future[Boolean]

produceRecords(YOUR_PRODUCE_RECORDS)
```

* Stop

Produce stop when all records produced or receive interrupt signal

#### KCL Consumer

* Constructor

```Scala
type RecordsHandlerType = Vector[Record] => Unit

object KclConsumer {
  def apply(profileName: String,
            regionName: String,
            streamName: String,
            consumeAppName: String,
            initialStreamPosition: InitialPositionInStream)
            (handler: RecordsHandlerType)

  def apply(profileName: String,
            regionName: String,
            streamName: String,
            consumeAppName: String)
            (handler: RecordsHandlerType)

  def apply(streamName: String,
            consumeAppName: String,
            initialStreamPosition: InitialPositionInStream)
            (handler: RecordsHandlerType)

  def apply(streamName: String,
            consumeAppName: String)
            (handler: RecordsHandlerType)
}
```

* Consume record

```Scala
val kclConsumer = KclConsumer(YOUR_STREAM_NAME, YOUR_CONSUMER_APP_NAME, YOUR_CONSUMER_INITIAL_POSITION_IN_STREAM)(YOUR_RECORDS_HANDLER)
// or
val kclConsumer = KclConsumer(YOUR_STREAM_NAME, YOUR_CONSUMER_APP_NAME, YOUR_CONSUMER_INITIAL_POSITION_IN_STREAM) {
  // Direct implement your records handler
  records:Vector[Record] => {
    // Your record handling code here.
  }
}

// start daemon consume worker.
kclConsumer.consumeRecords()
```

* Stop

Consumer stop when receive interrupt signal.
```Scala
def startGracefulShutdown(): util.concurrent.Future[lang.Boolean]
```
Before the app shuts down, you can use the gracefullShutdown method to clean up the consumer daemon.


## Build

Unit testing requires AWS credentials.
If you do not have AWS credentials, skip the test.

#### Build

- ./gradlew build

#### Build skip test

- ./gradlew assemble


## Test run

Test process
1. Reading the records from the file (example/example_record.txt)
2. Writing them in the kinesis stream (Producer)
3. Re-reading them and outputting to the screen and writing to the file in example dir (Consumer)
4. If the log message is too busy, modify the log level (by logback)

#### Run example by gradlew

- run example library mode : ./gradlew task library
- run example api mode : ./gradlew task api

#### Run example by jar

- run example library mode : java -jar ./build/libs/aws-kinesis-lib-scala-1.0-SNAPSHOT.jar library
- run example api mode : java -jar ./build/libs/aws-kinesis-lib-scala-1.0-SNAPSHOT.jar api

[SDK API Doc]:  https://docs.aws.amazon.com/kinesis/latest/APIReference/Welcome.html

[SDK Doc]: https://docs.aws.amazon.com/streams/latest/dev/working-with-streams.html
[KCL Doc]: https://docs.aws.amazon.com/streams/latest/dev/developing-consumers-with-kcl.html
[KPL Doc]: https://docs.aws.amazon.com/streams/latest/dev/developing-producers-with-kpl.html

[SDK Git]: https://github.com/aws/aws-sdk-java
[KCL Git]: https://github.com/awslabs/amazon-kinesis-client
[KPL Git]: https://github.com/awslabs/amazon-kinesis-producer
