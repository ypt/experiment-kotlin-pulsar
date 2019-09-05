package experiment.kotlin.pulsar

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.client.impl.schema.JSONSchema

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
fun schemaExample(client: PulsarClient) {
    println("PRODUCER 1 SETUP")
    val producer = client.newProducer<MySchema>(JSONSchema.of(MySchema::class.java))
            .topic("topic-with-myschema")
            .create()
    producer.send(MySchema("hello1", 1))
    producer.send(MySchema("hello2", 2))

    //
    // Pulsar supports schema versioning
    // Generally it allows new schema versions to expand (but not contract) upon previous versions
    // More info here: https://pulsar.apache.org/docs/en/concepts-schema-registry/#schema-versions
    //

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
    // Manually serializing json and sending to producer
    //
    val mapper = ObjectMapper().registerKotlinModule()

    // Sending a schema conformant json string as a byte array
    // This works as expected
    val json = mapper.writeValueAsString(MySchema2("hello7", 7, "seven"))
    println("PRODUCER 4 SETUP")
    val producer4 = client.newProducer()
            .topic("topic-with-myschema")
            .create()
    producer4.send(json.toByteArray())

    // Now let's try a payload that does not match the established schema...
    // Unfortunately, this write to Pulsar succeeds even though we don't want it to. It seems like data validation for
    // schema conformance is only enforced by the Pulsar client and not server side.
    val json2 = mapper.writeValueAsString(MyInvalidSchema(8, "NON CONFORMANT PAYLOAD"))
    println("PRODUCER 5 SETUP")
    val producer5 = client.newProducer()
            .topic("topic-with-myschema")
            .create()
    producer5.send(json2.toByteArray())
    producer5.send("Just a plain ol string. This definitely does not match the schema".toByteArray())

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
    //
    //    PRODUCER 1 SETUP
    //    PRODUCER 2 SETUP
    //    PRODUCER 3 SETUP
    //    org.apache.pulsar.client.api.PulsarClientException$IncompatibleSchemaException: org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException: Incompatible schema used
    //    PRODUCER 4 SETUP
    //    PRODUCER 5 SETUP
    //    CONSUMER SETUP
    //    STARTING CONSUME LOOP
    //    Message received: {"field1":"hello1","field2":1}
    //    Message received: {"field1":"hello2","field2":2}
    //    Message received: {"field1":"hello3","field2":3,"field3":"new field"}
    //    Message received: {"field1":"hello4","field2":4,"field3":"new field"}
    //    Message received: {"field1":"hello7","field2":7,"field3":"seven"}
    //    Message received: {"field100":8,"field200":"NON CONFORMANT PAYLOAD"}
    //    Message received: Just a plain ol string. This definitely does not match the schema
}
