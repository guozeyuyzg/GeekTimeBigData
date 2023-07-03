package realTimeCalcu.loginMonitor;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class LoginMontioring {
    public static void main(String[] args) throws Exception {
        //todo: 1、构建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "cdh06:9092,cdh07:9092,cdh08:9092");
        properties.put("group.id", "consumer-01");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("auto.offset.reset", "latest");

        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer<>("", new SimpleStringSchema(), properties));

        //todo: 3、数据处理分组
        KeyedStream<LoginInfo, Tuple> keyedStream = dataStreamSource.map(new MapFunction<String, LoginInfo>() {
            public LoginInfo map(String value) throws Exception {
                String[] users = value.split(",");
                return new LoginInfo(users[0], users[1], users[2], users[3]);
            }
        }).keyBy("username");

        //todo: 4、定义Parttern
        Pattern<LoginInfo, LoginInfo> pattern = Pattern.<LoginInfo>begin("start").where( new SimpleCondition<LoginInfo>() {
            public boolean filter(LoginInfo value) throws Exception {
                if (value.username != null) {
                    return true;
                }
                return false;
            }
        }).next("second").where(new IterativeCondition<LoginInfo>() {
            @Override
            public boolean filter(LoginInfo second, Context<LoginInfo> ctx) throws Exception {

                Iterable<LoginInfo> start = ctx.getEventsForPattern("start");
                Iterator<LoginInfo> userLoginIterator = start.iterator();
                while (userLoginIterator.hasNext()) {
                    LoginInfo userLogin = userLoginIterator.next();
                    if (!second.ip.equals(userLogin.ip)) {
                        return true;
                    }
                }
                return false;
            }
        });

        //todo: 5、将Parttern应用到事件流中进行检测，同时指定时间类型
        PatternStream<LoginInfo> patternStream = CEP.pattern(keyedStream, pattern).inProcessingTime();

        //todo: 6、提取匹配到的数据
        patternStream.select(new PatternSelectFunction<LoginInfo, LoginInfo>() {

            public LoginInfo select(Map<String, List<LoginInfo>> patternMap) throws Exception {

                //Map[String, util.List[(String, UserLoginInfo)]]
                //todo: key就是定义规则的名称：start  second
                //todo: value就是满足对应规则的数据

                List<LoginInfo> start = patternMap.get("start");
                List<LoginInfo> second = patternMap.get("second");

                LoginInfo startData = start.iterator().next();
                LoginInfo secondData = second.iterator().next();

                System.out.println("满足start模式中的数据 ：" + startData);
                System.out.println("满足second模式中的数据：" + secondData);

                return secondData;
            }
        });

        env.execute("LoginMonitor");
    }
    public static class LoginInfo {
        public  String ip;
        public  String username;
        public  String operateUrl;
        public  String time;

        //无参构造必须带上
        public LoginInfo() {}

        public LoginInfo(String ip, String username, String operateUrl, String time) {
            this.ip = ip;
            this.username = username;
            this.operateUrl = operateUrl;
            this.time = time;
        }

        @Override
        public String toString() {
            return ip+"\t"+username+"\t"+operateUrl+"\t"+time;
        }
    }

}
