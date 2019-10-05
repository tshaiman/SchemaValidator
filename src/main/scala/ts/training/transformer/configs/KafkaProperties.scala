package ts.training.transformer.configs

import java.util.Properties

import com.typesafe.config.Config
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.serializers.subject.RecordNameStrategy
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig

trait KafkaConfig extends ConfigSystem {
  private val kafkaConfig = config.getConfig("kafka")
  val inputTopic: String = configOrEnv("kafka.topics.in-json")
  val destTopic: String = configOrEnv("kafka.topics.out-avro")
  val dlqTopic: String = configOrEnv("kafka.topics.dlq-json")
  val appId: String = configOrEnv("kafka.topics.app-id")
  val clientId: String = configOrEnv("kafka.topics.client-id")

  val brokers: String = configOrEnv("kafka.bootstrap.servers")
  val schemaUrl: String = configOrEnv("kafka.schema.registry.url")

  val producerConfig: Config = kafkaConfig.getConfig("producer")
  val producerBufferMemoryBytes: String = producerConfig.getString("buffer.memory")
  val producerCompressionType: String = producerConfig.getString("compression.type")
  val producerBatchSize: String = producerConfig.getString("batch.size")
  val enableIdempotence: String = producerConfig.getString("enable.idempotence")
  val lingerMs: String = producerConfig.getString("linger.ms")


  val consumerConfig: Config = kafkaConfig.getConfig("consumer")
  val consumerPollTimeoutMs: Long = consumerConfig.getLong("poll-timeout-ms")
  val consumerEnableAutoCommit: String = consumerConfig.getString("enable.auto.commit")
  val consumerAutoOffsetReset: String = consumerConfig.getString("auto.offset.reset")
  val consumerMetadataMaxAgeMs: String = consumerConfig.getString("metadata.max.age.ms")
  val consumerFetchMaxWaitMs: String = consumerConfig.getString("fetch.max.wait.ms")
  val consumerMaxPollRecords: String = consumerConfig.getString("max.poll.records")
  val consumerMaxPartitionFetchBytes: String = consumerConfig.getString("max.partition.fetch.bytes")


}

object KafkaProperties extends KafkaConfig {

  def streamConfig: Properties = {

    val streamsConfiguration = new Properties()
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appId)
    streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, clientId)
    streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaUrl)
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[GenericAvroSerde])
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray.getClass.getName)
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, (5 * 1000).asInstanceOf[java.lang.Integer])
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0.asInstanceOf[java.lang.Integer])
    streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, "false")
    streamsConfiguration.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once")

    mergeProducerConsumerProps(streamsConfiguration)
    streamsConfiguration
  }

  private def mergeProducerConsumerProps(props: Properties): Properties = {
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, producerBufferMemoryBytes)
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, producerCompressionType)
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enableIdempotence)
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, producerBatchSize)
    props.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs)


    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, consumerEnableAutoCommit)
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, consumerMaxPollRecords)
    props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, consumerFetchMaxWaitMs)
    props
  }


}
