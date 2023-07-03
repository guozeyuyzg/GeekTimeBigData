package realTimeCalcu.mostHits;


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class MostHits {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "cdh06:9092,cdh07:9092,cdh08:9092");
        properties.put("group.id", "consumer-01");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("auto.offset.reset", "latest");

        DataStreamSource<String> dataStreamSource = environment.addSource(new FlinkKafkaConsumer<>("", new SimpleStringSchema(), properties));
        SingleOutputStreamOperator<UserBehavior> dataSource = dataStreamSource.map(new MapFunction<String, UserBehavior>() {
                                                                                       @Override
                                                                                       public UserBehavior map(String value) throws Exception {
                                                                                           String[] lineSplit = value.split(",");
                                                                                           UserBehavior userBehavior = new UserBehavior(Long.parseLong(lineSplit[0]), Long.parseLong(lineSplit[1]), Long.parseLong(lineSplit[2]), lineSplit[3], Long.parseLong(lineSplit[4]));
                                                                                           return userBehavior;
                                                                                       }
                                                                                   }
        );
        SingleOutputStreamOperator<UserBehavior> timestampData = dataSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior element) {
                //原始数据单位秒，将其转成毫秒
                return element.timestamp * 1000;
            }
        });

        WindowedStream<UserBehavior, Tuple, TimeWindow> userBehaviorTupleTimeWindowWindowedStream = timestampData.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior value) throws Exception {
                return value.behavior.equals("pv");
            }
        }).keyBy("itemId").timeWindow(Time.hours(1), Time.minutes(5));


        SingleOutputStreamOperator<String> topHits = userBehaviorTupleTimeWindowWindowedStream
                // 定义窗口聚合规则(这里不是来一条就累加，而是到达滑动间隔时，将该间隔【5分钟】的数据进行累加)， 定义输出数据结构
                .aggregate(new CountAgg(), new ItemCountWindowResult())
                .keyBy("windowEnd").process(new TopNHits(5));

        topHits.print();
        environment.execute();
    }
}
