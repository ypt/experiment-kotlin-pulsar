package experiment.kotlin.pulsar

import org.apache.pulsar.client.api.PulsarClient

fun main(args: Array<String>) {
    val client = PulsarClient.builder()
            .serviceUrl("pulsar://localhost:6650")
            .build()

    // Uncomment items below to try them out independently

    // schemaExample(client)
    // multiTopicSubscriptionExample(client)
    asyncExample(client)

    // In real usage, we should close producers, consumers, clients when they are no longer needed
    // producer.close()
    // consumer.close()
    // client.close()
}
