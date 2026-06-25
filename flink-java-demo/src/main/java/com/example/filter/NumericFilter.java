package com.example.filter;

import java.io.Serializable;
import java.util.Deque;

public interface NumericFilter extends Serializable {

    Deque<Double> apply(Deque<Double> values);
}