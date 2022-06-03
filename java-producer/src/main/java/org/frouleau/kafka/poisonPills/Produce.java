package org.frouleau.kafka.poisonPills;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import models.avro.SimpleValue;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Properties;

public class Produce {

    private static final Logger LOGGER = LogManager.getLogger(MethodHandles.lookup().lookupClass());

    private static final String DRIVER_ID = "Producer";
    private static final String TOPIC = "topic";
    private static final int NB_MESSAGES = 10;

    private static Properties settings() {
        final Properties settings = new Properties();
        settings.put(ProducerConfig.CLIENT_ID_CONFIG, DRIVER_ID);
        settings.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        settings.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://schema-registry:8081");
        return settings;
    }

    public static void main(String[] args) throws IOException {
        LOGGER.info("Starting producer");

        final KafkaProducer<String, SimpleValue> producer = new KafkaProducer<>(settings());

        // Adding a shutdown hook to clean up when the application exits
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Closing producer.");
            producer.close();
        }));

        for(int i=0; i < NB_MESSAGES; i++) {
            String key = Integer.toString(i);
            SimpleValue value = SimpleValue.newBuilder()
                    .setTheName("This is message " + key)
                    .build();
            ProducerRecord<String, SimpleValue> producerRecord = new ProducerRecord<>(TOPIC, key, value);
            LOGGER.info("Sending message {}", i);
            producer.send(producerRecord);
        }
        LOGGER.info("Producer flush");
        producer.flush();
    }

}