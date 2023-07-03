package realTimeCalcu.mostHits;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class ItemCountWindowResult implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {

    @Override
    public void apply(Tuple key, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
        out.collect(new ItemViewCount(Long.parseLong(key.getField(0)), window.getEnd(), input.iterator().next()));
    }
}
