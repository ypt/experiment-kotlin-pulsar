package experiment.kotlin.pulsar

import example.avro.User
import java.util.UUID
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.SchemaNormalization
import org.apache.avro.generic.GenericData
import org.apache.avro.message.BinaryMessageDecoder
import org.apache.avro.message.SchemaStore
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.common.policies.data.SchemaAutoUpdateCompatibilityStrategy

fun avroWithoutPulsarSchemaRegistryExample(client: PulsarClient, adminClient: PulsarAdmin) {
    val topic = UUID.randomUUID().toString()
    fun printSchemaInfo(namespace: String) {
        println("schemaValidationEnforced: ${adminClient.namespaces().getSchemaValidationEnforced(namespace)}")
        println("schemaAutoUpdateCompatibilityStrategy: ${adminClient.namespaces().getSchemaAutoUpdateCompatibilityStrategy(namespace)}")
    }

    println("PULSAR SCHEMA CONFIG")
    val schemaJson = """
        {"namespace": "example.avro",
         "type": "record",
         "name": "User",
         "fields": [
             {"name": "name", "type": "string"},
             {"name": "favorite_number",  "type": ["int", "null"]},
             {"name": "favorite_color", "type": ["string", "null"]}
         ]
        }
    """.trimIndent()
    val schemaParsedFromString = Schema.Parser().parse(schemaJson)
    adminClient.namespaces().setSchemaValidationEnforced("public/default", false)
    adminClient.namespaces().setSchemaAutoUpdateCompatibilityStrategy("public/default", SchemaAutoUpdateCompatibilityStrategy.AutoUpdateDisabled)
    printSchemaInfo("public/default")

    println("ACTUAL AVRO OBJECT SCHEMA")
    println(User.`SCHEMA$`.toString(true))

    println("SCHEMA FINGERPRINTS")
    // The Avro spec describes a way to encode a single object along with a fingerprint of its schema (instead of
    // the full schema). This was designed for messaging system use cases, where often, the schema itself is too large
    // to include in every single message.
    //
    // For more information, see:
    // - Avro spec: https://avro.apache.org/docs/current/spec.html#single_object_encoding_spec
    // - Java usage example: https://github.com/apache/avro/blob/master/lang/java/avro/src/test/java/org/apache/avro/message/TestBinaryMessageEncoding.java
    println("Fingerprint of User.`SCHEMA\$`: ${SchemaNormalization.parsingFingerprint64(User.`SCHEMA$`)}")
    println("Fingerprint of schema string: ${SchemaNormalization.parsingFingerprint64(schemaParsedFromString)}")

    println("PRODUCER SETUP")
    // Producer is set up for just sending plain old byte arrays. We'll need to do the work to serialize objects into
    // byte arrays.
    val producer = client.newProducer()
        .topic(topic)
        .create()

    println("CONSUMER SETUP")
    // Consumer is set up for just returning plain old byte arrays. We'll need to do the work to deserialize from byte
    // arrays into objects
    val consumer = client.newConsumer()
        .topic(topic)
        .subscriptionName("my-subscription")
        .subscribe()

    Thread {
        println("STARTING CONSUME LOOP")

        // Here's a decoder that that will read using the schema of the User class. That schema also happens to be the
        // writer schema, too.
        val decoder = User.getDecoder()

        // We can also use a different schema for reading. Here is an example of dynamically building a schema. The
        // decoder that we build with this schema will only know the reader schema, but not the writer schema (for now).
        val readerSchema = SchemaBuilder.record("User").fields()
            .requiredString("name")
            .endRecord()
        val decoder2 = BinaryMessageDecoder<GenericData.Record>(GenericData.get(), readerSchema)

        // Finding a schema when we only have a schema fingerprint is what we're interested in. It is abstracted as the
        // `SchemaStore` provider interface. The main purpose of a schema store is to look up writer schema given a schema
        // fingerprint.
        //
        // We can implement our own `SchemaStore` as necessary - we just need to implement the `findByFingerprint` function.
        //
        // See:
        // https://github.com/apache/avro/blob/master/lang/java/avro/src/main/java/org/apache/avro/message/SchemaStore.java
        //
        // Here, we just use the built in `SchemaScore.Cache` impl to simulate having a schema in the schema store.
        val schemaStore = SchemaStore.Cache()
        schemaStore.addSchema(schemaParsedFromString)
        val decoder3 = BinaryMessageDecoder<GenericData.Record>(GenericData.get(), readerSchema, schemaStore)

        while (true) {
            // Wait for a message
            val msg = consumer.receive()

            try {
                // Do something with the message
                println("msg.data: ${String(msg.data)}")
                println("msg.value: ${msg.value}")

                // This decoder knows about the writer schema
                println("DECODING WITH DECODER THAT KNOWS WRITER SCHEMA, USING User.`SCHEMA\$` READER SCHEMA")
                val decodedMessge = decoder.decode(msg.data)
                println("decoded by decoder: $decodedMessge")

                println("DECODING WITH DECODER THAT DOES NOT KNOW WRITER SCHEMA, USING DIFFERENT READER SCHEMA THAN WRITER SCHEMA")
                try {
                    val decodedMessage2 = decoder2.decode(msg.data)
                    println("decoded by decoder2: $decodedMessage2")
                } catch (e: Exception) {
                    // decoder2 does not know the writer schema, so this expectedly results in the following exception
                    // org.apache.avro.message.MissingSchemaException: Cannot resolve schema for fingerprint: -3588479540582100558
                    println(e)
                }

                // Now let's tell decoder2 about the writer schema, and try decoding again. The decoder can now find a
                // schema that matches the schema fingerprint included in the message. Decoding should work now.
                decoder2.addSchema(schemaParsedFromString)
                val decodedMessage2 = decoder2.decode(msg.data)
                println("decoded by decoder2: $decodedMessage2")

                // decoder3 was set up with a SchemaStore that already knows about the writer schema, so this should also work
                val decodedMessage3 = decoder3.decode(msg.data)
                println("decoded by decoder3: $decodedMessage3")

                // Acknowledge the message so that it can be deleted by the message broker
                consumer.acknowledge(msg)
            } catch (e: Exception) {
                println(e)
                // Message failed to process, redeliver later
                consumer.negativeAcknowledge(msg)
            }
        }
    }.start()

    Thread.sleep(500)
    val user = User("name", 100, "blue")
    val encoder = User.getEncoder()

    // Converting a byte buffer to a byte array
    val buffer = encoder.encode(user)
    val message = ByteArray(buffer.remaining())
    buffer.get(message, 0, message.size)

    producer.send(message)
}
