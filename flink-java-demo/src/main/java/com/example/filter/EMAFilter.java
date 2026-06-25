package com.example.filter;

import java.util.ArrayDeque;
import java.util.Deque;

public class EMAFilter implements NumericFilter {

    private final double alpha;

    public EMAFilter(double alpha) {
        this.alpha = alpha;
    }

    @Override
    public Deque<Double> apply(Deque<Double> values) {
        if (values.isEmpty()) {
            return new ArrayDeque<>();
        }

        Deque<Double> result = new ArrayDeque<>();

        Double previous = null;

        for (double value : values) {
            if (previous == null) {
                previous = value;
            } else {
                previous = alpha * value + (1 - alpha) * previous;
            }

            result.add(previous);
        }

        return result;
    }
}