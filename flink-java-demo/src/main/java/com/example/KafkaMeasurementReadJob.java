package com.example;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaMeasurementReadJob {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env
                = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("192.168.6.163:9092")
                .setTopics("raw-measurements")
                .setGroupId("flink-measurement-reader-v2")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> kafkaJsonStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka Raw Measurement Source"
        );

        DataStream<MeasurementPayload> measurementStream = kafkaJsonStream
                .map(json -> objectMapper.readValue(json, MeasurementPayload.class));

        measurementStream
                .keyBy(measurement -> measurement.tagId)
                .process(new SimpleAggregationFunction(3, 3))
                .print();

        env.execute("Kafka Measurement Read Job");
    }

    public static class AggregatedMeasurement {

        public String anchorId;
        public double avgAzimuth;
        public double avgElevation;
        public double avgDistance;
        public long timestamp;

        public AggregatedMeasurement() {
            // Required by  serializFlinkation
        }

        public AggregatedMeasurement(
                String anchorId,
                double avgAzimuth,
                double avgElevation,
                double avgDistance,
                long timestamp
        ) {
            this.anchorId = anchorId;
            this.avgAzimuth = avgAzimuth;
            this.avgElevation = avgElevation;
            this.avgDistance = avgDistance;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "AggregatedMeasurement{"
                    + "anchorId='" + anchorId + '\''
                    + ", avgAzimuth=" + avgAzimuth
                    + ", avgElevation=" + avgElevation
                    + ", avgDistance=" + avgDistance
                    + ", timestamp=" + timestamp
                    + '}';
        }
    }

    public static class AggregatedMeasurementSet {

        public String tagId;
        public List<AggregatedMeasurement> measurements;

        public AggregatedMeasurementSet() {
            // Required by Flink serialization
        }

        public AggregatedMeasurementSet(String tagId, List<AggregatedMeasurement> measurements) {
            this.tagId = tagId;
            this.measurements = measurements;
        }

        @Override
        public String toString() {
            return "AggregatedMeasurementSet{"
                    + "tagId='" + tagId + '\''
                    + ", measurements=" + measurements
                    + '}';
        }
    }

    public static class AnchorSignalWindow {

        private int windowSize;
        private LinkedList<MeasurementPayload> measurements;

        public AnchorSignalWindow() {
            // Required by Flink serialization
        }

        public AnchorSignalWindow(int windowSize) {
            this.windowSize = windowSize;
            this.measurements = new LinkedList<>();
        }

        public void add(MeasurementPayload measurement) {
            if (measurements == null) {
                measurements = new LinkedList<>();
            }

            measurements.add(measurement);

            if (measurements.size() > windowSize) {
                measurements.removeFirst();
            }
        }

        public boolean isReady() {
            return measurements != null && measurements.size() >= windowSize;
        }

        public AggregatedMeasurement average(String anchorId) {
            double azimuthSum = 0.0;
            double elevationSum = 0.0;
            double distanceSum = 0.0;

            long latestTimestamp = 0L;

            for (MeasurementPayload measurement : measurements) {
                azimuthSum += measurement.azimuth;
                elevationSum += measurement.elevation;
                distanceSum += measurement.distance;

                latestTimestamp = Math.max(latestTimestamp, measurement.timestamp);
            }

            int count = measurements.size();

            return new AggregatedMeasurement(
                    anchorId,
                    azimuthSum / count,
                    elevationSum / count,
                    distanceSum / count,
                    latestTimestamp
            );
        }
    }

    public static class SimpleAggregationFunction
            extends KeyedProcessFunction<String, MeasurementPayload, AggregatedMeasurementSet> {

        private final int expectedAnchors;
        private final int windowSize;

        private transient MapState<String, AnchorSignalWindow> anchorWindows;

        public SimpleAggregationFunction(int expectedAnchors, int windowSize) {
            this.expectedAnchors = expectedAnchors;
            this.windowSize = windowSize;
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            MapStateDescriptor<String, AnchorSignalWindow> descriptor
                    = new MapStateDescriptor<>(
                            "anchor-windows",
                            String.class,
                            AnchorSignalWindow.class
                    );

            anchorWindows = getRuntimeContext().getMapState(descriptor);
        }

        @Override
        public void processElement(
                MeasurementPayload measurement,
                Context context,
                Collector<AggregatedMeasurementSet> out
        ) throws Exception {

            String anchorId = measurement.anchorId;

            AnchorSignalWindow window = anchorWindows.get(anchorId);

            if (window == null) {
                window = new AnchorSignalWindow(windowSize);
            }

            window.add(measurement);

            anchorWindows.put(anchorId, window);

            if (isReady()) {
                AggregatedMeasurementSet result = buildAggregatedResult(measurement.tagId);
                out.collect(result);
            }
        }

        private boolean isReady() throws Exception {
            int readyAnchorCount = 0;

            for (AnchorSignalWindow window : anchorWindows.values()) {
                if (window.isReady()) {
                    readyAnchorCount++;
                }
            }

            return readyAnchorCount >= expectedAnchors;
        }

        private AggregatedMeasurementSet buildAggregatedResult(String tagId) throws Exception {
            List<AggregatedMeasurement> aggregatedMeasurements = new ArrayList<>();

            for (String anchorId : anchorWindows.keys()) {
                AnchorSignalWindow window = anchorWindows.get(anchorId);

                if (window != null && window.isReady()) {
                    aggregatedMeasurements.add(window.average(anchorId));
                }
            }

            return new AggregatedMeasurementSet(tagId, aggregatedMeasurements);
        }
    }

    public static class MeasurementPayload {

        public String tagId;
        public String anchorId;

        public double azimuth;
        public double azimuth_stdev;
        public double elevation;
        public double elevation_stdev;
        public double distance;
        public double distance_stdev;
        public long sequence;
        public long timestamp;

        public MeasurementPayload() {
        }

        @Override
        public String toString() {
            return "MeasurementPayload{"
                    + "tagId='" + tagId + '\''
                    + ", anchorId='" + anchorId + '\''
                    + ", azimuth=" + azimuth
                    + ", elevation=" + elevation
                    + ", distance=" + distance
                    + ", sequence=" + sequence
                    + ", timestamp=" + timestamp
                    + '}';
        }

    }
}
