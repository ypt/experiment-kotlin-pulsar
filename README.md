# experiment-kotlin-pulsar
A proof of concept experimenting with a few [Pulsar](https://pulsar.apache.org/en/) concepts in Kotlin, particularly:

1. [Schemas](https://pulsar.apache.org/docs/en/schema-get-started/)
2. [Multi-topic subscriptions](https://pulsar.apache.org/docs/en/client-libraries-java/#multi-topic-subscriptions)

## Try it out
Start Pulsar [via Docker](https://pulsar.apache.org/docs/en/standalone-docker/)

```
$ docker run -it \
  -p 6650:6650 \
  -p 8080:8080 \
  -v $PWD/data:/pulsar/data \
  apachepulsar/pulsar:2.4.1 \
  bin/pulsar standalone
```

Build and run this app
```
$ ./gradlew run
```

To try the different examples, uncomment them out in `experiment/kotlin/pulsar/App.kt`