/*
 * Copyright (c) 2014-2016 by its authors. Some rights reserved.
 * See the project homepage at: https://github.com/monixio/monix-kafka
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package monix.kafka

import java.io.File
import java.util.Properties
import scala.concurrent.duration._

import com.typesafe.config.{Config, ConfigFactory}

import monix.kafka.config._

/** The Kafka Producer config.
  *
  * For the official documentation on the available configuration
  * options, see
  * [[https://kafka.apache.org/082/documentation.html#newproducerconfigs Producer Configs]]
  * on `kafka.apache.org`.
  *
  * @param bootstrapServers is the `bootstrap.servers` setting
  *        and represents the list of servers to connect to.
  *
  * @param acks is the `acks` setting and represents
  *        the number of acknowledgments the producer requires the leader to
  *        have received before considering a request complete.
  *        See [[monix.kafka.config.Acks Acks]].
  *
  * @param bufferMemoryInBytes is the `buffer.memory` setting and
  *        represents the total bytes of memory the producer
  *        can use to buffer records waiting to be sent to the server.
  *
  * @param compressionType is the `compression.type` setting and specifies
  *        what compression algorithm to apply to all the generated data
  *        by the producer. The default is none (no compression applied).
  *
  * @param retries is the `retries` setting. A value greater than zero will
  *        cause the client to resend any record whose send fails with
  *        a potentially transient error.
  *
  * @param batchSizeInBytes is the `batch.size` setting.
  *        The producer will attempt to batch records together into fewer
  *        requests whenever multiple records are being sent to the
  *        same partition. This setting specifies the maximum number of
  *        records to batch together.
  *
  * @param clientId is the `client.id` setting,
  *        an id string to pass to the server when making requests.
  *        The purpose of this is to be able to track the source of
  *        requests beyond just ip/port by allowing a logical application
  *        name to be included in server-side request logging.
  *
  * @param lingerTime is the `linger.ms` setting
  *        and specifies to buffer records for more efficient batching,
  *        up to the maximum batch size or for the maximum `lingerTime`.
  *        If zero, then no buffering will happen, but if different
  *        from zero, then records will be delayed in absence of load.
  *
  * @param maxRequestSizeInBytes is the `max.request.size` setting
  *        and represents the maximum size of a request in bytes.
  *        This is also effectively a cap on the maximum record size.
  *
  * @param receiveBufferInBytes is the `receive.buffer.bytes` setting
  *        being the size of the TCP receive buffer (SO_RCVBUF) to use
  *        when reading data.
  *
  * @param sendBufferInBytes is the `send.buffer.bytes` setting,
  *        being the size of the TCP send buffer (SO_SNDBUF) to use
  *        when sending data.
  *
  * @param timeout is the `timeout.ms` setting,
  *        a configuration the controls the maximum amount of time
  *        the server will wait for acknowledgments from followers to meet
  *        the acknowledgment requirements the producer has specified with
  *        the `acks` configuration.
  *
  * @param blockOnBufferFull is the `block.on.buffer.full` setting,
  *        which controls whether producer stops accepting new
  *        records (blocks) or throws errors when the memory buffer
  *        is exhausted.
  *
  * @param metadataFetchTimeout is the `metadata.fetch.timeout.ms` setting.
  *        The period of time in milliseconds after which we force a
  *        refresh of metadata even if we haven't seen any partition
  *        leadership changes to proactively discover any new brokers
  *        or partitions.
  *
  * @param metadataMaxAge is the `metadata.max.age.ms` setting.
  *        The period of time in milliseconds after which we force a
  *        refresh of metadata even if we haven't seen any partition
  *        leadership changes to proactively discover any new brokers
  *        or partitions.
  *
  * @param reconnectBackoffTime is the `reconnect.backoff.ms` setting.
  *        The amount of time to wait before attempting to reconnect to a
  *        given host. This avoids repeatedly connecting to a host in a
  *        tight loop. This backoff applies to all requests sent by the
  *        consumer to the broker.
  *
  * @param metricReporters is the `metric.reporters` setting.
  *         A list of classes to use as metrics reporters. Implementing the
  *         `MetricReporter` interface allows plugging in classes that will
  *         be notified of new metric creation. The JmxReporter is always
  *         included to register JMX statistics
  *
  * @param metricsNumSamples is the `metrics.num.samples` setting.
  *         The number of samples maintained to compute metrics.
  *
  * @param metricsSampleWindow is the `metrics.sample.window.ms` setting.
  *         The metrics system maintains a configurable number of samples over
  *         a fixed window size. This configuration controls the size of the
  *         window. For example we might maintain two samples each measured
  *         over a 30 second period. When a window expires we erase and
  *         overwrite the oldest window.
  *
  * @param retryBackoffTime is the `retry.backoff.ms` setting.
  *        The amount of time to wait before attempting to retry a failed
  *        request to a given topic partition. This avoids repeatedly
  *        sending requests in a tight loop under some failure scenarios.
  *
  * @param monixSinkParallelism is the `monix.producer.sink.parallelism`
  *        setting indicating how many requests the [[KafkaProducerSink]]
  *        can execute in parallel.
  */
case class KafkaProducerConfig(
  bootstrapServers: List[String],
  acks: Acks,
  bufferMemoryInBytes: Int,
  compressionType: CompressionType,
  retries: Int,
  batchSizeInBytes: Int,
  clientId: String,
  lingerTime: FiniteDuration,
  maxRequestSizeInBytes: Int,
  receiveBufferInBytes: Int,
  sendBufferInBytes: Int,
  timeout: FiniteDuration,
  blockOnBufferFull: Boolean,
  metadataFetchTimeout: FiniteDuration,
  metadataMaxAge: FiniteDuration,
  metricReporters: List[String],
  metricsNumSamples: Int,
  metricsSampleWindow: FiniteDuration,
  reconnectBackoffTime: FiniteDuration,
  retryBackoffTime: FiniteDuration,
  monixSinkParallelism: Int) {

  def toProperties: Properties = {
    val props = new Properties()
    for ((k, v) <- toMap; if v != null) props.put(k, v)
    props
  }

  def toMap: Map[String, String] = Map(
    "bootstrap.servers" -> bootstrapServers.mkString(","),
    "acks" -> acks.id,
    "buffer.memory" -> bufferMemoryInBytes.toString,
    "compression.type" -> compressionType.id,
    "retries" -> retries.toString,
    "batch.size" -> batchSizeInBytes.toString,
    "client.id" -> clientId,
    "linger.ms" -> lingerTime.toMillis.toString,
    "max.request.size" -> maxRequestSizeInBytes.toString,
    "receive.buffer.bytes" -> receiveBufferInBytes.toString,
    "send.buffer.bytes" -> sendBufferInBytes.toString,
    "timeout.ms" -> timeout.toMillis.toString,
    "block.on.buffer.full" -> blockOnBufferFull.toString,
    "metadata.fetch.timeout.ms" -> metadataFetchTimeout.toMillis.toString,
    "metadata.max.age.ms" -> metadataMaxAge.toMillis.toString,
    "metric.reporters" -> metricReporters.mkString(","),
    "metrics.num.samples" -> metricsNumSamples.toString,
    "metrics.sample.window.ms" -> metricsSampleWindow.toMillis.toString,
    "reconnect.backoff.ms" -> reconnectBackoffTime.toMillis.toString,
    "retry.backoff.ms" -> retryBackoffTime.toMillis.toString
  )
}

object KafkaProducerConfig {
  private val defaultRootPath = "kafka"

  lazy private val defaultConf: Config =
    ConfigFactory.load("monix/kafka/default.conf").getConfig(defaultRootPath)

  /** Returns the default configuration, specified the `monix-kafka` project
    * in `monix/kafka/default.conf`.
    */
  lazy val default: KafkaProducerConfig =
    apply(defaultConf, includeDefaults = false)

  /** Loads the [[KafkaProducerConfig]] either from a file path or
    * from a resource, if `config.file` or `config.resource` are
    * defined, or otherwise returns the default config.
    *
    * If you want to specify a `config.file`, you can configure the
    * Java process on execution like so:
    * {{{
    *   java -Dconfig.file=/path/to/application.conf
    * }}}
    *
    * Or if you want to specify a `config.resource` to be loaded
    * from the executable's distributed JAR or classpath:
    * {{{
    *   java -Dconfig.resource=com/company/mySpecial.conf
    * }}}
    *
    * In case neither of these are specified, then the configuration
    * loaded is the default one, from the `monix-kafka` project, specified
    * in `monix/kafka/default.conf`.
    */
  def load(): KafkaProducerConfig =
    Option(System.getProperty("config.file")).map(f => new File(f)) match {
      case Some(file) if file.exists() =>
        loadFile(file)
      case None =>
        Option(System.getProperty("config.resource")) match {
          case Some(resource) =>
            loadResource(resource)
          case None =>
            default
        }
    }

  /** Loads a [[KafkaProducerConfig]] from a project resource.
    *
    * @param resourceBaseName is the resource from where to load the config
    * @param rootPath is the config root path (e.g. `kafka`)
    * @param includeDefaults should be `true` in case you want to fallback
    *        to the default values provided by the `monix-kafka` library
    *        in `monix/kafka/default.conf`
    */
  def loadResource(resourceBaseName: String, rootPath: String = defaultRootPath, includeDefaults: Boolean = true): KafkaProducerConfig =
    apply(ConfigFactory.load(resourceBaseName).getConfig(rootPath), includeDefaults)

  /** Loads a [[KafkaProducerConfig]] from a specified file.
    *
    * @param file is the configuration path from where to load the config
    * @param rootPath is the config root path (e.g. `kafka`)
    * @param includeDefaults should be `true` in case you want to fallback
    *        to the default values provided by the `monix-kafka` library
    *        in `monix/kafka/default.conf`
    */
  def loadFile(file: File, rootPath: String = defaultRootPath, includeDefaults: Boolean = true): KafkaProducerConfig =
    apply(ConfigFactory.parseFile(file).resolve().getConfig(rootPath), includeDefaults)

  /** Loads the [[KafkaProducerConfig]] from a parsed
    * `com.typesafe.config.Config` reference.
    *
    * NOTE that this method doesn't assume any path prefix for loading the
    * configuration settings, so it does NOT assume a root path like `kafka`.
    * In case case you need that, you can always do:
    *
    * {{{
    *   KafkaProducerConfig(globalConfig.getConfig("kafka"))
    * }}}
    *
    * @param source is the typesafe `Config` object to read from
    * @param includeDefaults should be `true` in case you want to fallback
    *        to the default values provided by the `monix-kafka` library
    *        in `monix/kafka/default.conf`
    */
  def apply(source: Config, includeDefaults: Boolean = true): KafkaProducerConfig = {
    val config = if (!includeDefaults) source else source.withFallback(defaultConf)

    KafkaProducerConfig(
      bootstrapServers = config.getString("bootstrap.servers").trim.split("\\s*,\\s*").toList,
      acks = Acks(config.getString("acks")),
      bufferMemoryInBytes = config.getInt("buffer.memory"),
      compressionType = CompressionType(config.getString("compression.type")),
      retries = config.getInt("retries"),
      batchSizeInBytes = config.getInt("batch.size"),
      clientId = config.getString("client.id"),
      lingerTime = config.getInt("linger.ms").millis,
      maxRequestSizeInBytes = config.getInt("max.request.size"),
      receiveBufferInBytes = config.getInt("receive.buffer.bytes"),
      sendBufferInBytes = config.getInt("send.buffer.bytes"),
      timeout = config.getInt("timeout.ms").millis,
      blockOnBufferFull = config.getBoolean("block.on.buffer.full"),
      metadataFetchTimeout = config.getInt("metadata.fetch.timeout.ms").millis,
      metadataMaxAge = config.getInt("metadata.max.age.ms").millis,
      metricReporters = config.getString("metric.reporters").trim.split("\\s*,\\s*").toList,
      metricsNumSamples = config.getInt("metrics.num.samples"),
      metricsSampleWindow = config.getInt("metrics.sample.window.ms").millis,
      reconnectBackoffTime = config.getInt("reconnect.backoff.ms").millis,
      retryBackoffTime = config.getInt("retry.backoff.ms").millis,
      monixSinkParallelism = config.getInt("monix.producer.sink.parallelism")
    )
  }
}
