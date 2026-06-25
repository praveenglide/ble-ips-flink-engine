package com.example.model;

public class AggregatedMeasurement {

    public String anchorId;

    public double avgAzimuth;
    public double avgElevation;
    public double avgDistance;

    public long timestamp;

    public AggregatedMeasurement() {
        // Required by Flink serialization
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
        return "AggregatedMeasurement{" +
                "anchorId='" + anchorId + '\'' +
                ", avgAzimuth=" + avgAzimuth +
                ", avgElevation=" + avgElevation +
                ", avgDistance=" + avgDistance +
                ", timestamp=" + timestamp +
                '}';
    }
}