package com.example.model;

public class MeasurementPayload {

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
        // Required by Jackson and Flink serialization
    }

    @Override
    public String toString() {
        return "MeasurementPayload{" +
                "tagId='" + tagId + '\'' +
                ", anchorId='" + anchorId + '\'' +
                ", azimuth=" + azimuth +
                ", azimuth_stdev=" + azimuth_stdev +
                ", elevation=" + elevation +
                ", elevation_stdev=" + elevation_stdev +
                ", distance=" + distance +
                ", distance_stdev=" + distance_stdev +
                ", sequence=" + sequence +
                ", timestamp=" + timestamp +
                '}';
    }
}