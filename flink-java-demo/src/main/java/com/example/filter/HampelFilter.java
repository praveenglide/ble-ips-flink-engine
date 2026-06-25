package com.example.filter;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;

public class HampelFilter implements NumericFilter {

    private final double k;
    private static final double EPSILON = 1e-6;

    public HampelFilter(double k) {
        this.k = k;
    }

    @Override
    public Deque<Double> apply(Deque<Double> values) {
        if (values.isEmpty()) {
            return new ArrayDeque<>();
        }

        List<Double> list = new ArrayList<>(values);

        List<Double> sorted = new ArrayList<>(list);
        Collections.sort(sorted);

        double median = sorted.get(sorted.size() / 2);

        List<Double> deviations = new ArrayList<>();

        for (double value : list) {
            deviations.add(Math.abs(value - median));
        }

        Collections.sort(deviations);

        double mad = deviations.get(deviations.size() / 2);

        if (mad < EPSILON) {
            mad = EPSILON;
        }

        double threshold = k * mad;

        Deque<Double> cleaned = new ArrayDeque<>();

        for (double value : list) {
            if (Math.abs(value - median) > threshold) {
                cleaned.add(median);
            } else {
                cleaned.add(value);
            }
        }

        return cleaned;
    }
}