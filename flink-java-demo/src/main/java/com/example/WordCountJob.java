package com.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountJob {

    public static void main(String[] args) throws Exception {

        // Create Flink execution environment
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // Set parallelism
        env.setParallelism(2);

        // Sample input data
        var input = env.fromData(
                "hello flink",
                "hello java",
                "flink is powerful",
                "java flink example"
        );

        // Word count logic
        input
                .flatMap(new SplitWords())
                .keyBy(value -> value.f0)
                .sum(1)
                .print();

        // Start Flink job
        env.execute("Simple Java Word Count Job");
    }

    public static class SplitWords implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
            String[] words = line.toLowerCase().split("\\s+");

            for (String word : words) {
                if (!word.isBlank()) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        }
    }
}