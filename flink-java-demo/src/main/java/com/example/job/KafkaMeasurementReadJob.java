package com.example.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.example.function.SimpleAggregationFunction;
import com.example.model.MeasurementPayload;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.example.model.AggregatedMeasurementSet;
import com.example.serialization.AggregatedMeasurementKafkaSerializer;

import org.apache.flink.connector.kafka.sink.KafkaSink;

public class KafkaMeasurementReadJob {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("192.168.6.163:9092")
                .setTopics("raw-measurements")
                .setGroupId("flink-measurement-reader-v2")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSink<AggregatedMeasurementSet> aggregatedMeasurementSink =
                KafkaSink.<AggregatedMeasurementSet>builder()
                        .setBootstrapServers("192.168.6.163:9092")
                        .setRecordSerializer(new AggregatedMeasurementKafkaSerializer())
                        .build();

        DataStream<String> kafkaJsonStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka Raw Measurement Source"
        );

        DataStream<MeasurementPayload> measurementStream = kafkaJsonStream
                .map(json -> objectMapper.readValue(json, MeasurementPayload.class));

        DataStream<AggregatedMeasurementSet> aggregatedStream = measurementStream
                .keyBy(measurement -> measurement.tagId)
                .process(new SimpleAggregationFunction(3, 6));

        aggregatedStream.print();

        aggregatedStream.sinkTo(aggregatedMeasurementSink);

        env.execute("Kafka Measurement Read Job");
    }
}