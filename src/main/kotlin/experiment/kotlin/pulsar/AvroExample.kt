package experiment.kotlin.pulsar

import example.avro.User
import java.util.UUID
import org.apache.avro.Schema.Parser
import org.apache.avro.SchemaNormalization
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.client.api.schema.SchemaDefinition
import org.apache.pulsar.common.policies.data.SchemaAutoUpdateCompatibilityStrategy
import org.apache.pulsar.common.schema.SchemaInfo
import org.apache.pulsar.common.schema.SchemaType

fun avroExample(client: PulsarClient, adminClient: PulsarAdmin) {
    val topic = UUID.randomUUID().toString()
    fun printSchemaInfo(namespace: String) {
        println("schemaValidationEnforced: ${adminClient.namespaces().getSchemaValidationEnforced(namespace)}")
        println("schemaAutoUpdateCompatibilityStrategy: ${adminClient.namespaces().getSchemaAutoUpdateCompatibilityStrategy(namespace)}")
        println("schemaInfo: ${adminClient.schemas().getSchemaInfo("public/default/$topic")}")
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
    adminClient.namespaces().setSchemaValidationEnforced("public/default", true)
    adminClient.namespaces().setSchemaAutoUpdateCompatibilityStrategy("public/default", SchemaAutoUpdateCompatibilityStrategy.AutoUpdateDisabled)
    adminClient.schemas().createSchema(
        "public/default/$topic",
        SchemaInfo(
            "schema-name",
            schemaJson.toByteArray(),
            SchemaType.AVRO,
            null
        )
    )
    printSchemaInfo("public/default")

    println("ACTUAL AVRO OBJECT SCHEMA")
    println(User.`SCHEMA$`.toString(true))

    println("SCHEMA FINGERPRINTS")
    println("Fingerprint of User.`SCHEMA\$`: ${SchemaNormalization.parsingFingerprint64(User.`SCHEMA$`)}")
    println("Fingerprint of schema string: ${SchemaNormalization.parsingFingerprint64(Parser().parse(schemaJson))}")

    println("PRODUCER 1 SETUP")
    // If you are using standardized Avro schema definitions across systems, when setting up Pulsar consumers and
    // producers, you should consider explicitly specifying the Avro schema as a JSON string, instead of letting Pulsar
    // generate schemas from the POJO.
    //
    // Specifically, is because the schema derived from Pulsar's POJO -> Avro schema generation logic may generate
    // a schema different than that which was defined in an Avro schema file.
    //
    // For example, given this process:
    // Schema A from Avro schema file -> code-gen'd Avro POJO -> Pulsar schema extraction -> Schema B
    // there's no guarantee that Schema A == Schema B
    //
    // This especially becomes an issue if we view Avro schemas as our standard schema specification used across systems.
    // Having Pulsar mutate a schema away from the standard schema specification when generating one from a POJO is not
    // acceptable in a world where we want to shuffle data between systems via Pulsar and at the same time leverage
    // standardized organization wide schema definitions.
    //
    // A solution should be available as of Pulsar 2.3.1
    //
    // See:
    // https://pulsar.apache.org/docs/en/schema-understand/
    // https://github.com/apache/pulsar/issues/3741#issuecomment-469506795
    // https://github.com/apache/pulsar/pull/3766
    //
    // So instead of this:
    // val producer = client.newProducer<User>(Schema.AVRO(User::class.java))
    //
    // Do the following to explicitly use the schema attached to an Avro class:

    // TODO: The following still isn't quite right. Look into why creating this producer still results in IncompatibleSchemaException
    val schemaDefinition = SchemaDefinition.builder<User>()
        .withJsonDef(schemaJson)
        .withAlwaysAllowNull(false)
        .build()
    val producer = client.newProducer<User>(Schema.AVRO(schemaDefinition))
        .topic(topic)
        .create()

    //
    // Consumer
    //
    println("CONSUMER SETUP")

    try {
        client.newConsumer<User>(Schema.AVRO(User::class.java))
            .topic(topic)
            .subscriptionName("my-subscription")
            .subscribe()
    } catch (e: Exception) {
        println(e)

        // The schema that Pulsar generates via Schema.AVRO(User::class.java) != User.getClassSchema()
        // so the following exception is thrown when trying to connect the consumer
        // org.apache.pulsar.client.api.PulsarClientException$IncompatibleSchemaException: Trying to subscribe with incompatible schema
    }

    val consumer = client.newConsumer<User>(Schema.AVRO(schemaDefinition))
        .topic(topic)
        .subscriptionName("my-subscription")
        .subscribe()

    Thread {
        println("STARTING CONSUME LOOP")
        while (true) {
            // Wait for a message
            val msg = consumer.receive()

            // The deserialized User object
            val user = msg.value

            try {
                // Do something with the message
                println("Message data: ${String(msg.data)}")
                println("Message User object: $user")
                println("Message User object: ${user.javaClass}")

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
    // val encoder = User.getEncoder()
    // val buffer = encoder.encode(user)
    // val message = ByteArray(buffer.remaining())
    // buffer.get(message, 0, message.size)
    producer.send(user)

    // TODO: test schema conversion and unknown schema lookup
}
