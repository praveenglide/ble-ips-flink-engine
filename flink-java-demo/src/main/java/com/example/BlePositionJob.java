package com.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class BlePositionJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        var bleEvents = env.fromData(
                new BleEvent("T1001", "A1", 45.0, -60),
                new BleEvent("T1002", "A2", 60.0, -55),
                new BleEvent("T1001", "A3", 30.0, -70),
                new BleEvent("T1003", "A1", 90.0, -50),
                new BleEvent("T1002", "A4", 15.0, -65)
        );

        bleEvents
                .filter(event -> event.rssi > -80)
                .keyBy(event -> event.tagId)
                .map(event -> {
                    double x = Math.cos(Math.toRadians(event.angle)) * Math.abs(event.rssi);
                    double y = Math.sin(Math.toRadians(event.angle)) * Math.abs(event.rssi);

                    String status = x > 40 ? "RESTRICTED_ZONE" : "SAFE";

                    return new PositionResult(event.tagId, x, y, status);
                })
                .print();

        env.execute("BLE Position Processing Job");
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