package realTimeCalcu.mostVisitUrl;

import org.apache.flink.api.common.functions.AggregateFunction;

public class UrlCountAgg implements AggregateFunction<LogEvent, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(LogEvent value, Long accumulator) {
        return accumulator + 1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }
}
