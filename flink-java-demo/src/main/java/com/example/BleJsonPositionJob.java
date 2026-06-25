package com.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.fasterxml.jackson.databind.ObjectMapper;

public class BleJsonPositionJob {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        var jsonInput = env.fromData(
                "{\"tagId\":\"T1001\",\"anchorId\":\"A1\",\"angle\":45.0,\"rssi\":-60}",
                "{\"tagId\":\"T1002\",\"anchorId\":\"A2\",\"angle\":60.0,\"rssi\":-55}",
                "{\"tagId\":\"T1001\",\"anchorId\":\"A3\",\"angle\":30.0,\"rssi\":-70}",
                "{\"tagId\":\"T1003\",\"anchorId\":\"A1\",\"angle\":90.0,\"rssi\":-50}",
                "{\"tagId\":\"T1002\",\"anchorId\":\"A4\",\"angle\":15.0,\"rssi\":-65}"
        );

        jsonInput
                .map(json -> objectMapper.readValue(json, BleEvent.class))
                .filter(event -> event.rssi > -80)
                .keyBy(event -> event.tagId)
                .map(event -> {
                    double x = Math.cos(Math.toRadians(event.angle)) * Math.abs(event.rssi);
                    double y = Math.sin(Math.toRadians(event.angle)) * Math.abs(event.rssi);

                    String status = x > 40 ? "RESTRICTED_ZONE" : "SAFE";

                    return new PositionResult(event.tagId, x, y, status);
                })
                .print();

        env.execute("BLE JSON Position Processing Job");
    }

    public static class BleEvent {
        public String tagId;
        public String anchorId;
        public double angle;
        public int rssi;

        public BleEvent() {
        }

        public BleEvent(String tagId, String anchorId, double angle, int rssi) {
            this.tagId = tagId;
            this.anchorId = anchorId;
            this.angle = angle;
            this.rssi = rssi;
        }

        @Override
        public String toString() {
            return "BleEvent{" +
                    "tagId='" + tagId + '\'' +
                    ", anchorId='" + anchorId + '\'' +
                    ", angle=" + angle +
                    ", rssi=" + rssi +
                    '}';
        }
    }

    public static class PositionResult {
        public String tagId;
        public double x;
        public double y;
        public String status;

        public PositionResult() {
        }

        public PositionResult(String tagId, double x, double y, String status) {
            this.tagId = tagId;
            this.x = x;
            this.y = y;
            this.status = status;
        }

        @Override
        public String toString() {
            return "PositionResult{" +
                    "tagId='" + tagId + '\'' +
                    ", x=" + x +
                    ", y=" + y +
                    ", status='" + status + '\'' +
                    '}';
        }
    }
}