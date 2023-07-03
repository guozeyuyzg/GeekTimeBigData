package realTimeCalcu;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.stream.Stream;

public class KafkaProducerUtil {
    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "cdh06:9092,cdh07:9092,cdh08:9092");
        //消息的确认机制
        properties.put("acks", "all");
        properties.put("retries", 0);

        //缓冲区的大小  //默认32M
        properties.put("buffer.memory", 33554432);
        //批处理数据的大小，每次写入多少数据到topic   //默认16KB
        properties.put("batch.size", 16384);
        //可以延长多久发送数据   //默认为0 表示不等待 ，立即发送
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        //指定数据序列化和反序列化
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> stringStringKafkaProducer = new KafkaProducer<>(properties);

        String topicName="user_behavior_test";
        String fileName = "D:\\tmp\\UserBehavior.csv";
        Stream<String> lines = Files.lines(Paths.get(fileName));
        lines.forEach(ele -> {
//            System.out.println("第一行数据为:"+ele);
            ProducerRecord<String, String> stringStringProducerRecord = new ProducerRecord<>(topicName, ele);
            stringStringKafkaProducer.send(stringStringProducerRecord);
        });
        stringStringKafkaProducer.close();

    }
}
