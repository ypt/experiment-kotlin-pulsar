package experiment.kotlin.pulsar

import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.api.PulsarClient

fun main(args: Array<String>) {
    val client = PulsarClient.builder()
            .serviceUrl("pulsar://localhost:6650")
            .build()

    val adminClient = PulsarAdmin.builder()
        .serviceHttpUrl("http://localhost:8080")
        .build()

    // Uncomment items below to try them out independently

    schemaExample(client, adminClient)
    // multiTopicSubscriptionExample(client)
    // asyncExample(client)

    // In real usage, we should close producers, consumers, clients when they are no longer needed
    // producer.close()
    // consumer.close()
    // client.close()
}
