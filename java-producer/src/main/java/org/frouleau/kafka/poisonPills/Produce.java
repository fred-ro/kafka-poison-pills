package org.frouleau.kafka.poisonPills;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import models.avro.SimpleValue;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class Produce {

    private static final Logger LOGGER = LogManager.getLogger(MethodHandles.lookup().lookupClass());

    private static final String DRIVER_ID = "Producer";
    private static final String TOPIC = "topic";
    private static final int NB_MESSAGES = 10;
    private int count = 0;

    private Properties settings() {
        final Properties settings = new Properties();
        settings.put(ProducerConfig.CLIENT_ID_CONFIG, DRIVER_ID);
        settings.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        settings.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        return settings;
    }
    
    private Properties avroSettings() {
        final Properties settings = settings();
        settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        return settings;
    }

    private Properties rawSettings() {
        final Properties settings = settings();
        settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return settings;
    }
    
    public static void main(String[] args) {
        Produce produce = new Produce();
        // Enable either the following line or the line after that
        produce.sendWrongAvroProducer(10);
        //produce.sendAvroProducer(10);
    }

    void sendAvroProducer(int nb) {
        LOGGER.info("Starting Arvo Producer");
        try (KafkaProducer<String, SimpleValue> producer = new KafkaProducer<>(avroSettings())) {
            for (int i=0; i < nb; i++) {
                String key = Integer.toString(count);
                SimpleValue value = SimpleValue.newBuilder()
                        .setTheName("This is message " + key)
                        .build();
                ProducerRecord<String, SimpleValue> producerRecord = new ProducerRecord<>(TOPIC, key, value);
                LOGGER.info("Sending message {}", count);
                producer.send(producerRecord, (RecordMetadata recordMetadata, Exception exception) -> {
                    if (exception == null) {
                        System.out.println("Record written to offset " +
                                recordMetadata.offset() + " timestamp " +
                                recordMetadata.timestamp());
                    } else {
                        System.err.println("An error occurred");
                        exception.printStackTrace(System.err);
                    }
              });
                count++;
            }
            LOGGER.info("Producer flush");
            producer.flush();
        } finally {
            LOGGER.info("Closing producer");
        }
    }

    void sendRawProducer(int nb) {
        LOGGER.info("Starting Raw Producer");
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(rawSettings())) {
            for (int i=0; i < nb; i++) {
                String key = Integer.toString(count);
                String value = "This is message " + key;
                var producerRecord = new ProducerRecord<>(TOPIC, key, value.getBytes(StandardCharsets.UTF_8));
                LOGGER.info("Sending message {}", count);
                producer.send(producerRecord, (RecordMetadata recordMetadata, Exception exception) -> {
                    if (exception == null) {
                        System.out.println("Record written to offset " +
                                recordMetadata.offset() + " timestamp " +
                                recordMetadata.timestamp());
                    } else {
                        System.err.println("An error occurred");
                        exception.printStackTrace(System.err);
                    }
                });
                count++;
            }
            LOGGER.info("Producer flush");
            producer.flush();
        } finally {
            LOGGER.info("Closing producer");
        }
    }

    void sendWrongAvroProducer(int nb) {
        LOGGER.info("Starting AvroWrong Producer");
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(rawSettings())) {
            for (int i=0; i < nb; i++) {
                String key = Integer.toString(count);
                SimpleValue value = SimpleValue.newBuilder()
                        .setTheName("This is message " + key)
                        .build();
                var producerRecord = new ProducerRecord<>(TOPIC, key, generateAvroValue(value, i != 3));
                LOGGER.info("Sending message {}", count);
                producer.send(producerRecord, (RecordMetadata recordMetadata, Exception exception) -> {
                    if (exception == null) {
                        System.out.println("Record written to offset " +
                                recordMetadata.offset() + " timestamp " +
                                recordMetadata.timestamp());
                    } else {
                        System.err.println("An error occurred");
                        exception.printStackTrace(System.err);
                    }
                });
                count++;
            }
            LOGGER.info("Producer flush");
            producer.flush();
        } catch (IOException e) {
            LOGGER.error("Failed to send", e);
        } finally {
            LOGGER.info("Closing producer");
        }
    }

    byte[] generateAvroValue(SimpleValue value, boolean valid) throws IOException {
        int id = valid ? 1 : 0;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(0); // Magic byte
        out.write(ByteBuffer.allocate(4).putInt(id).array()); // Schema Id
        DatumWriter<SimpleValue> simpleValueDatumWriter = new SpecificDatumWriter<>(SimpleValue.getClassSchema());
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        simpleValueDatumWriter.write(value, encoder);
        encoder.flush();
        byte[] bytes = out.toByteArray();
        out.close();
        return bytes;
    }

}
