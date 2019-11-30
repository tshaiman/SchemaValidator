package ts.training.transformer.configs

import java.util.Properties

import com.typesafe.config.Config
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
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
  val producerCompressionType: String = producerConfig.getString("compression.type")
  val producerBatchSize: String = producerConfig.getString("batch.size")
  val enableIdempotence: String = producerConfig.getString("enable.idempotence")
  val lingerMs: String = producerConfig.getString("linger.ms")
  val producerMaxRequestSize: String = producerConfig.getString("max.request.size")


  val consumerConfig: Config = kafkaConfig.getConfig("consumer")
  val consumerMaxPollRecords: String = consumerConfig.getString("max.poll.records")
  val consumerFetchMaxBytes: String = consumerConfig.getString("fetch.max.bytes")


}

object KafkaProperties extends KafkaConfig {

  def streamConfig: Properties = {

    val streamsConfiguration = new Properties()
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appId)
    streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, clientId)
    streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaUrl)
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[GenericAvroSerde])
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, (5 * 1000).asInstanceOf[java.lang.Integer])
    streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0.asInstanceOf[java.lang.Integer])
    streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, "false")
    streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1")
    streamsConfiguration.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)

    //Consumer  dedicated Configs
    streamsConfiguration.put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_RECORDS_CONFIG), consumerMaxPollRecords)
    streamsConfiguration.put(StreamsConfig.consumerPrefix(ConsumerConfig.FETCH_MAX_BYTES_CONFIG), consumerFetchMaxBytes)

    //Producer dedicated Configs
    streamsConfiguration.put(StreamsConfig.producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG), producerCompressionType)
    streamsConfiguration.put(StreamsConfig.producerPrefix(ProducerConfig.MAX_REQUEST_SIZE_CONFIG), producerMaxRequestSize)
    streamsConfiguration.put(StreamsConfig.producerPrefix(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG), enableIdempotence)
    streamsConfiguration.put(StreamsConfig.producerPrefix(ProducerConfig.BATCH_SIZE_CONFIG), producerBatchSize)
    streamsConfiguration.put(StreamsConfig.producerPrefix(ProducerConfig.LINGER_MS_CONFIG), lingerMs)

    streamsConfiguration
  }


}
