package training.transformer.schema

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.Schema
import training.transformer.configs.{ConfigSystem, KafkaConfig}


object SchemaRepository
  extends ConfigSystem
    with KafkaConfig {

  val client: CachedSchemaRegistryClient = new CachedSchemaRegistryClient(schemaUrl, 100)

  def getSchema(topic: String): Schema = {
    val subject = s"$topic-value"
    val metadata = client.getLatestSchemaMetadata(subject)
    val schema = new org.apache.avro.Schema.Parser().parse(metadata.getSchema)
    //register it to the cache
    client.register(subject,schema)


    schema
  }


}