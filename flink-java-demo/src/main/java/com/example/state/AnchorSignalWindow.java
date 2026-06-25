package com.example.state;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;

import com.example.filter.FilterPipeline;
import com.example.model.AggregatedMeasurement;
import com.example.model.MeasurementPayload;

public class AnchorSignalWindow {

    private int windowSize;

    private Deque<Double> azimuthValues;
    private Deque<Double> elevationValues;
    private Deque<Double> distanceValues;

    private long latestTimestamp;

    public AnchorSignalWindow() {
        // Required by Flink serialization
    }

    public AnchorSignalWindow(int windowSize) {
        this.windowSize = windowSize;
        this.azimuthValues = new ArrayDeque<>();
        this.elevationValues = new ArrayDeque<>();
        this.distanceValues = new ArrayDeque<>();
        this.latestTimestamp = 0L;
    }

    public void add(MeasurementPayload measurement) {
        initializeIfNeeded();

        addToWindow(azimuthValues, measurement.azimuth);
        addToWindow(elevationValues, measurement.elevation);
        addToWindow(distanceValues, measurement.distance);

        latestTimestamp = Math.max(latestTimestamp, measurement.timestamp);
    }

    private void initializeIfNeeded() {
        if (azimuthValues == null) {
            azimuthValues = new ArrayDeque<>();
        }

        if (elevationValues == null) {
            elevationValues = new ArrayDeque<>();
        }

        if (distanceValues == null) {
            distanceValues = new ArrayDeque<>();
        }
    }

    private void addToWindow(Deque<Double> window, double value) {
        window.addLast(value);

        if (window.size() > windowSize) {
            window.removeFirst();
        }
    }

    public boolean isReady() {
        return azimuthValues != null
                && elevationValues != null
                && distanceValues != null
                && azimuthValues.size() >= windowSize
                && elevationValues.size() >= windowSize
                && distanceValues.size() >= windowSize;
    }

    public Deque<Double> getAzimuthValuesSnapshot() {
        initializeIfNeeded();
        return new ArrayDeque<>(azimuthValues);
    }

    public Deque<Double> getElevationValuesSnapshot() {
        initializeIfNeeded();
        return new ArrayDeque<>(elevationValues);
    }

    public Deque<Double> getDistanceValuesSnapshot() {
        initializeIfNeeded();
        return new ArrayDeque<>(distanceValues);
    }

    public long latestTimestamp() {
        return latestTimestamp;
    }

    public AggregatedMeasurement average(String anchorId) {
        double avgAzimuth = averageOf(azimuthValues);
        double avgElevation = averageOf(elevationValues);
        double avgDistance = averageOf(distanceValues);

        return new AggregatedMeasurement(
                anchorId,
                avgAzimuth,
                avgElevation,
                avgDistance,
                latestTimestamp
        );
    }

    public AggregatedMeasurement aggregateWithFilters(
            String anchorId,
            FilterPipeline filterPipeline
    ) {
        Deque<Double> azimuthRaw = getAzimuthValuesSnapshot();
        Deque<Double> elevationRaw = getElevationValuesSnapshot();
        Deque<Double> distanceRaw = getDistanceValuesSnapshot();

        Deque<Double> azimuthPrepared = filterPipeline.apply(azimuthRaw);
        Deque<Double> elevationPrepared = filterPipeline.apply(elevationRaw);
        Deque<Double> distancePrepared = filterPipeline.apply(distanceRaw);

        double finalAzimuth = medianOf(azimuthPrepared);
        double finalElevation = medianOf(elevationPrepared);
        double finalDistance = medianOf(distancePrepared);

        return new AggregatedMeasurement(
                anchorId,
                finalAzimuth,
                finalElevation,
                finalDistance,
                latestTimestamp
        );
    }

    private double medianOf(Deque<Double> values) {
        if (values == null || values.isEmpty()) {
            return 0.0;
        }

        List<Double> sorted = new ArrayList<>(values);
        Collections.sort(sorted);

        return sorted.get(sorted.size() / 2);
    }

    private double averageOf(Deque<Double> values) {
        if (values == null || values.isEmpty()) {
            return 0.0;
        }

        double sum = 0.0;

        for (double value : values) {
            sum += value;
        }

        return sum / values.size();
    }

    @Override
    public String toString() {
        return "AnchorSignalWindow{"
                + "windowSize=" + windowSize
                + ", azimuthValues=" + azimuthValues
                + ", elevationValues=" + elevationValues
                + ", distanceValues=" + distanceValues
                + ", latestTimestamp=" + latestTimestamp
                + '}';
    }
}
