# kafka-poison-pills

## Building the code

Building the code using gradle:

```bash
./gradlew build
```

Potentially, you might need to update gradle first:

```bash
gradle wrapper
```

## Running the code

First, start the docker environment:

```bash
docker-compose up -d
```


Run producer with:

```bash
./gradlew :java-producer:run
```

Run consumer with:

```bash
./gradlew :java-consumer:run
```

Read messages via CLI tools:

```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic topic --from-beginning
```

You might want to delete the topic to start fresh between tests:

```bash
kafka-topics --bootstrap-server localhost:9092 --delete --topic topic
```


## Shutting down, deleting containers

```bash
docker-compose down
docker-compose rm
```

