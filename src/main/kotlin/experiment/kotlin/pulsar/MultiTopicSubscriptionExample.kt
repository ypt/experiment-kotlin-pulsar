package experiment.kotlin.pulsar

import java.util.regex.Pattern
import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.client.api.Schema

//
// https://pulsar.apache.org/docs/en/client-libraries-java/#multi-topic-subscriptions
//
fun multiTopicSubscriptionExample(client: PulsarClient) {
    println("PRODUCER 1 SETUP")
    val producer = client.newProducer<String>(Schema.STRING)
            .topic("multi-topic-example-topic-1")
            .create()
    producer.send("topic-1, message-1")
    producer.send("topic-1, message-2")

    println("PRODUCER 2 SETUP")
    val producer2 = client.newProducer<String>(Schema.STRING)
            .topic("multi-topic-example-topic-2")
            .create()
    producer2.send("topic-2, message-1")
    producer2.send("topic-2, message-2")

    println("PRODUCER 3 SETUP")
    val producer3 = client.newProducer<String>(Schema.STRING)
            .topic("multi-topic-example-topic-3")
            .create()
    producer3.send("topic-3, message-1")
    producer3.send("topic-3, message-2")

    //
    // Consumer
    //

    // We can subscribe to multiple topics via a regex pattern or a list of topics
    // More info here: https://pulsar.apache.org/docs/en/client-libraries-java/#multi-topic-subscriptions
    println("CONSUMER SETUP")
    val consumer = client.newConsumer()
            .topicsPattern(Pattern.compile("persistent://public/default/multi-topic-example-topic-.*"))
            // We can also subscribe to an explicit list of topics, if necessary, like so:
            // .topics(listOf("multi-topic-example-topic-1", "multi-topic-example-topic-2"))
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

    // Running the above will print something _similar to_ the following:
    //
    //   PRODUCER 1 SETUP
    //   PRODUCER 2 SETUP
    //   PRODUCER 3 SETUP
    //   CONSUMER SETUP
    //   STARTING CONSUME LOOP
    //   Message received: topic-2, message-1
    //   Message received: topic-2, message-2
    //   Message received: topic-3, message-1
    //   Message received: topic-1, message-1
    //   Message received: topic-3, message-2
    //   Message received: topic-1, message-2
    //
    // Note that _within_ a topic, messages are received in the same order as in which they were published. However,
    // _between_ topics we are not guaranteed to receive messages the same as the order in which they were published.
}
