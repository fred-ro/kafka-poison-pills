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

Register the schema:

```bash
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
--data '{"schema": "{\"namespace\": \"models.avro\", \"type\": \"record\", \"name\": \"SimpleValue\", \"fields\": [ {\"name\": \"theName\", \"type\": \"string\"}]}"}' \
http://localhost:8081/subjects/topic-value/versions
```

Get all known versions:

```bash
curl -H "Content-Type: application/vnd.schemaregistry.v1+json" http://localhost:8081/subjects/topic-value/versions
```

Inspect a specific version (here: version 1):

```bash
curl -H "Content-Type: application/vnd.schemaregistry.v1+json" http://localhost:8081/subjects/topic-value/versions/1
```

Run producer with gradle:

```bash
./gradlew :java-producer:run
```

Run consumer with:

```bash
./gradlew :java-consumer:run
```

Read messages via CLI tools, using standard console consumer:

```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic topic --from-beginning
```
Read messages via avro console consumer (except to see exceptions!):

```bash
kafka-avro-console-consumer --bootstrap-server localhost:9092 --property schema.registry.url=http://localhost:8081 --topic topic --from-beginning
```

You might want to delete the topic to start fresh between tests:

```bash
kafka-topics --bootstrap-server localhost:9092 --delete --topic topic
```

Alternatively, if you just want to consume the same messages again with the Java consumer, just reset the consumer groups offset:

```bash
kafka-consumer-groups --bootstrap-server localhost:9092 --group Consumer --reset-offsets --to-earliest --topic topic --execute
```

You can view the offsets by running:

```bash
kafka-consumer-groups --bootstrap-server localhost:9092 --group Consumer --describe
```

## Experimenting
Update the producer and try the different methods.

## Shutting down, deleting containers

```bash
docker-compose down
docker-compose rm
```

