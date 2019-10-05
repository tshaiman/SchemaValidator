package ts.training.transformer

import java.time.Duration

import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.ExtendedJsonDecoder
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder}
import org.slf4j.LoggerFactory
import ts.training.transformer.configs.{ConfigSystem, KafkaConfig, KafkaProperties}
import ts.training.transformer.schema.SchemaRepository

import scala.util.{Failure, Success, Try}

//scripts
/*Create new topic for raw data
bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic products-avro --replication-factor 1 --partitions 1
bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic products-raw --replication-factor 1 --partitions 1
*/

  /* Producer some raw json data
kafkacat -P -b 0 -t products-raw
==and then use the following inpt
{"name":"Floor Lamp","catalogNr":"503.237.62","description":"a bamboo standing lamp","price":49.99},
{"name":"Baby Blanket","catalogNr":"804.271.12","description":"Soft Snag blanket","price":12.99},
{"name":"Room Curtains","catalogNr":"604.189.05","description":"Room darkening curtains prevent most light from entering","price":59.12}
*/

/*Create new Schema
curl -X POST -H 'Content-Type:application/vnd.schemaregistry.v1+json'  \
--data '{"schema":"{\"type\":\"record\",\"name\":\"products\",\"namespace\":\"ts.com.training\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}, {\"name\":\"catalogNr\",\"type\":\"string\"},{\"name\":\"description\",\"type\":\"string\"},{\"name\":\"price\",\"type\":\"float\"}]}"}'  \
http://localhost:8081/subjects/products-avro-value2/versions
*/


/*check the output topic using avro tools
kafka-avro-console-consumer --bootstrap-server localhost:9092 \                                                                                                                                                 19:32  
--property schema.registry.url=http://localhost:8081 \
--topic products-avro  \
--from-beginning  | \
jq '.'

*/

object TrasnformerApp extends App
  with ConfigSystem
  with KafkaConfig {


  val logger = LoggerFactory.getLogger("transformer")

  val streams = buildStream()
  streams.start()

  sys.ShutdownHookThread {
    streams.close(Duration.ofSeconds(10))
  }

  def buildStream(): KafkaStreams = {

    logger.info(s"Starting KStream Json to Avro Transformer. input: $inputTopic , output: $destTopic")
    val builder = new StreamsBuilder
    val raw: KStream[Array[Byte], String] = builder.stream(inputTopic, Consumed.`with`(Serdes.ByteArray, Serdes.String))

    val maybeTransfomed: KStream[Array[Byte], Either[String, GenericRecord]] = raw.mapValues(v => transformToAvro(v))
    val transformed: KStream[Array[Byte], GenericRecord] = maybeTransfomed.filter((_, v) => v.isRight).mapValues(v => v.right.get)
    val errorStream: KStream[Array[Byte], String] = maybeTransfomed.filter((_, v) => v.isLeft).mapValues(v => v.left.get)

    transformed.to(destTopic)
    errorStream.to(dlqTopic,Produced.`with`(Serdes.ByteArray(),Serdes.String()))

    transformed.print(Printed.toSysOut[Array[Byte],GenericRecord].withLabel("success"))
    errorStream.print(Printed.toSysOut[Array[Byte],String].withLabel("fails"))

    new KafkaStreams(builder.build, KafkaProperties.streamConfig)
  }


  def transformToAvro(rawJson: String): Either[String, GenericRecord] = Try {
    val schema = SchemaRepository.getSchema(destTopic)
    val reader: GenericDatumReader[GenericRecord] = new GenericDatumReader[GenericRecord](schema)
    val decoder = new ExtendedJsonDecoder(schema, rawJson)
    val gr: GenericRecord = reader.read(null, decoder)
    gr
  } match {
    case Success(x) => Right(x)
    case Failure(e) => {
      logger.error("could not convert to generic record", e)
      Left(rawJson)
    }
  }

}
