package com.example.function;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import com.example.filter.EMAFilter;
import com.example.filter.FilterPipeline;
import com.example.filter.HampelFilter;
import com.example.model.AggregatedMeasurement;
import com.example.model.AggregatedMeasurementSet;
import com.example.model.MeasurementPayload;
import com.example.state.AnchorSignalWindow;

public class SimpleAggregationFunction
        extends KeyedProcessFunction<String, MeasurementPayload, AggregatedMeasurementSet> {

    private final int expectedAnchors;
    private final int windowSize;

    private transient MapState<String, AnchorSignalWindow> anchorWindows;

    private transient FilterPipeline filterPipeline;

    public SimpleAggregationFunction(int expectedAnchors, int windowSize) {
        this.expectedAnchors = expectedAnchors;
        this.windowSize = windowSize;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        MapStateDescriptor<String, AnchorSignalWindow> descriptor
                = new MapStateDescriptor<>(
                        "anchor-windows",
                        String.class,
                        AnchorSignalWindow.class
                );

        anchorWindows = getRuntimeContext().getMapState(descriptor);

        filterPipeline = new FilterPipeline(
                List.of(
                        new HampelFilter(1.5),
                        new EMAFilter(0.3)
                )
        );

    }

    @Override
    public void processElement(
            MeasurementPayload measurement,
            Context context,
            Collector<AggregatedMeasurementSet> out
    ) throws Exception {

        String anchorId = measurement.anchorId;

        AnchorSignalWindow window = anchorWindows.get(anchorId);

        if (window == null) {
            window = new AnchorSignalWindow(windowSize);
        }

        window.add(measurement);

        anchorWindows.put(anchorId, window);

        if (isReady()) {
            AggregatedMeasurementSet result = buildAggregatedResult(measurement.tagId);
            out.collect(result);
        }
    }

    private boolean isReady() throws Exception {
        int readyAnchorCount = 0;

        for (AnchorSignalWindow window : anchorWindows.values()) {
            if (window.isReady()) {
                readyAnchorCount++;
            }
        }

        return readyAnchorCount >= expectedAnchors;
    }

    private AggregatedMeasurementSet buildAggregatedResult(String tagId) throws Exception {
        List<AggregatedMeasurement> aggregatedMeasurements = new ArrayList<>();

        for (String anchorId : anchorWindows.keys()) {
            AnchorSignalWindow window = anchorWindows.get(anchorId);

            if (window != null && window.isReady()) {
                aggregatedMeasurements.add(
                        window.aggregateWithFilters(anchorId, filterPipeline)
                );;
            }
        }

        return new AggregatedMeasurementSet(tagId, aggregatedMeasurements);
    }
}
