package com.example.serialization;

import java.nio.charset.StandardCharsets;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema.KafkaSinkContext;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.example.model.AggregatedMeasurementSet;
import com.fasterxml.jackson.databind.ObjectMapper;

public class AggregatedMeasurementKafkaSerializer
        implements KafkaRecordSerializationSchema<AggregatedMeasurementSet> {

    private static final String TOPIC_NAME = "aggregated-measurements";

    private transient ObjectMapper objectMapper;

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            AggregatedMeasurementSet element,
            KafkaSinkContext context,
            Long timestamp
    ) {
        try {
            if (objectMapper == null) {
                objectMapper = new ObjectMapper();
            }

            byte[] key = element.tagId.getBytes(StandardCharsets.UTF_8);
            byte[] value = objectMapper.writeValueAsBytes(element);

            return new ProducerRecord<>(
                    TOPIC_NAME,
                    key,
                    value
            );

        } catch (Exception exception) {
            throw new RuntimeException(
                    "Failed to serialize AggregatedMeasurementSet for Kafka",
                    exception
            );
        }
    }
}