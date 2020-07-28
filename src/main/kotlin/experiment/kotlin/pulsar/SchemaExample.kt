package experiment.kotlin.pulsar

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.client.impl.schema.JSONSchema
import org.apache.pulsar.common.policies.data.SchemaAutoUpdateCompatibilityStrategy

data class MySchema(
    val field1: String,
    val field2: Int
)

data class MySchema2(
    val field1: String,
    val field2: Int,
    val field3: String
)

data class MyInvalidSchema(
    val field100: Int,
    val field200: String
)

//
// https://pulsar.apache.org/docs/en/client-libraries-java/#schemas
//
fun schemaExample(client: PulsarClient, adminClient: PulsarAdmin) {
    fun printSchemaPolicies(namespace: String) {
        println("schemaValidationEnforced: ${adminClient.namespaces().getSchemaValidationEnforced(namespace)}")
        println("schemaAutoUpdateCompatibilityStrategy: ${adminClient.namespaces().getSchemaAutoUpdateCompatibilityStrategy(namespace)}")
    }

    println("PRODUCER 1 SETUP")
    val producer = client.newProducer<MySchema>(JSONSchema.of(MySchema::class.java))
        .topic("topic-with-myschema")
        .create()
    producer.send(MySchema("hello1", 1))
    producer.send(MySchema("hello2", 2))

    //
    // Pulsar supports schema evolution policies
    // Various compatibility modes can be configured
    // https://pulsar.apache.org/docs/en/schema-evolution-compatibility/
    //
    println("INIT SCHEMA POLICIES")
    adminClient.namespaces().setSchemaValidationEnforced("public/default", true)
    adminClient.namespaces().setSchemaAutoUpdateCompatibilityStrategy("public/default", SchemaAutoUpdateCompatibilityStrategy.Full)
    printSchemaPolicies("public/default")

    // Using a new compatible schema
    println("PRODUCER 2 SETUP")
    val producer2 = client.newProducer<MySchema2>(JSONSchema.of(MySchema2::class.java))
        .topic("topic-with-myschema")
        .create()
    producer2.send(MySchema2("hello3", 3, "new field"))
    producer2.send(MySchema2("hello4", 4, "new field"))

    // Using an incompatible schema will result in an exception during creation of the producer
    println("PRODUCER 3 SETUP")
    try {
        val producer3 = client.newProducer<MyInvalidSchema>(JSONSchema.of(MyInvalidSchema::class.java))
            .topic("topic-with-myschema")
            .create()

        producer3.send(MyInvalidSchema(100, "hello5"))
        producer3.send(MyInvalidSchema(200, "hello6"))
    } catch (e: Exception) {
        println(e)

        // The following exception is thrown when creating the above producer, which specifies an incompatible schema:
        // org.apache.pulsar.client.api.PulsarClientException$IncompatibleSchemaException: org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException: Incompatible schema used
    }

    //
    // Sending to producer
    //
    val mapper = ObjectMapper().registerKotlinModule()

    // What happens if schema validation IS enforced, but a producer does NOT come with a schema?
    // The following producer that does not have a schema is NOT allowed to connect and produce a message
    // even if the data itself actually aligns with the allowed schema
    val json = mapper.writeValueAsString(MySchema2("hello7", 7, "seven"))
    println("PRODUCER 4 SETUP")
    try {
        val producer4 = client.newProducer()
            .topic("topic-with-myschema")
            .create()
        producer4.send(json.toByteArray())
    } catch (e: Exception) {
        println(e)

        // The following exception is thrown when creating the above producer
        // org.apache.pulsar.client.api.PulsarClientException$IncompatibleSchemaException: org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException: Producers cannot connect without a schema to topics with a schema
    }
    // Unfortunately, the above succeeds.
    // To actually be more strict about enforcing schemas, we need to:
    // 1. enable `schemaValidationEnforced` at the namespace or cluster - https://pulsar.apache.org/docs/en/schema-manage/#schema-validation
    // 2. use a Pulsar client that supports the Pulsar schema registry - https://pulsar.apache.org/docs/en/schema-get-started/#schema-registry

    // Now what happens if schema validation is disabled?
    // The following producer that does not have a schema IS allowed to connect and produce a message
    println("UPDATING SCHEMA POLICIES")
    adminClient.namespaces().setSchemaValidationEnforced("public/default", false)
    printSchemaPolicies("public/default")

    val producer4 = client.newProducer()
        .topic("topic-with-myschema")
        .create()
    producer4.send(json.toByteArray())

    //
    // Consumer
    //
    println("CONSUMER SETUP")
    val consumer = client.newConsumer()
        .topic("topic-with-myschema")
        .subscriptionName("my-subscription")
        .subscribe()

    println("STARTING CONSUME LOOP")
    while (true) {
        // Wait for a message
        val msg = consumer.receive()

        try {
            // Do something with the message
            println("Message received: ${String(msg.data)}")

            // Acknowledge the message so that it can be deleted by the message broker
            consumer.acknowledge(msg)
        } catch (e: Exception) {
            println(e)
            // Message failed to process, redeliver later
            consumer.negativeAcknowledge(msg)
        }
    }

    // Running the above will print the following:
    //     PRODUCER 1 SETUP
    //     INIT SCHEMA POLICIES
    //     schemaValidationEnforced: true
    //     schemaAutoUpdateCompatibilityStrategy: Full
    //     PRODUCER 2 SETUP
    //     PRODUCER 3 SETUP
    //     org.apache.pulsar.client.api.PulsarClientException$IncompatibleSchemaException: org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException: Incompatible schema used
    //     PRODUCER 4 SETUP
    //     org.apache.pulsar.client.api.PulsarClientException$IncompatibleSchemaException: org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException: Producers cannot connect without a schema to topics with a schema
    //     UPDATING SCHEMA POLICIES
    //     schemaValidationEnforced: false
    //     schemaAutoUpdateCompatibilityStrategy: Full
    //     CONSUMER SETUP
    //     STARTING CONSUME LOOP
    //     Message received: {"field1":"hello1","field2":1}
    //     Message received: {"field1":"hello2","field2":2}
    //     Message received: {"field1":"hello3","field2":3,"field3":"new field"}
    //     Message received: {"field1":"hello4","field2":4,"field3":"new field"}
    //     Message received: {"field1":"hello7","field2":7,"field3":"seven"}
}
