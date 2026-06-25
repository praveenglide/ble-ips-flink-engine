package com.example.filter;

import java.util.Deque;
import java.util.List;

public class FilterPipeline implements NumericFilter {

    private final List<NumericFilter> filters;

    public FilterPipeline(List<NumericFilter> filters) {
        this.filters = filters;
    }

    @Override
    public Deque<Double> apply(Deque<Double> values) {
        Deque<Double> result = values;

        for (NumericFilter filter : filters) {
            result = filter.apply(result);
        }

        return result;
    }
}