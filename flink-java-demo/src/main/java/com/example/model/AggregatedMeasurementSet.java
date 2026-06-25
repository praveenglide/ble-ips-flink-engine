package com.example.model;

import java.util.List;

public class AggregatedMeasurementSet {

    public String tagId;
    public List<AggregatedMeasurement> measurements;

    public AggregatedMeasurementSet() {
        // Required by Flink serialization
    }

    public AggregatedMeasurementSet(
            String tagId,
            List<AggregatedMeasurement> measurements
    ) {
        this.tagId = tagId;
        this.measurements = measurements;
    }

    @Override
    public String toString() {
        return "AggregatedMeasurementSet{" +
                "tagId='" + tagId + '\'' +
                ", measurements=" + measurements +
                '}';
    }
}