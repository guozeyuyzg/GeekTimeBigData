package hudi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkWithHudi {
    public static void main(String[] args) {
//1.获取表的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //并行度设置为1
        env.setParallelism(1);
        //TODO: 由于增量将数据写入到Hudi表，所以需要启动Flink Checkpoint 检查点
        env.enableCheckpointing(5 * 1000);


        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()//设置流式模式
                //  .useBlinkPlanner()
                .build();
        //flink当中的tableAPI
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


        //2.创建输入表，TODO:从kafka消费数据
        tableEnv.executeSql(
                "CREATE TABLE order_kafka_source(\n" +
                        "    `orderId` STRING,\n" +
                        "    `userId` STRING,\n" +
                        "    `orderTime` STRING,\n" +
                        "    `ip` STRING,\n" +
                        "    `orderMoney` DOUBLE,\n" +
                        "    `orderStatus` INT\n" +
                        ")\n" +
                        "WITH(\n" +
                        "    'connector' = 'kafka',\n" +
                        "    'topic'='user_behivor',\n" +//user_behivor
                        "    'properties.bootstrap.servers' = 'cdh06:9092,cdh07:9092,cdh08:9092',\n" +
                        "    'properties.group.id' = 'gid-1001',\n" +
                        "    'scan.startup.mode' = 'earliest-offset',\n" +//latest-offset
                        "    'value.format' = 'json',\n" +
                        "    'value.json.fail-on-missing-field' = 'false',\n" +
                        "    'value.json.ignore-parse-errors' = 'true'\n" +
                        ")\n"
        );

        //3.转换数据，可以使用SQL，也可以是TableAPI
        Table etlTable = tableEnv
                .from("order_kafka_source")
                //添加字段：hudi数据合并的字段，时间戳  "orderId":"20230516140147594000001"
                .addColumns(
                        $("orderId").substring(0, 17).as("ts")
                )
                //添加字段：Hudi表分区字段，"orderTime": 2022-03-09 22:21:13.124
                .addColumns(
                        $("orderTime").substring(0, 10).as("partition_day")
                );


        tableEnv.createTemporaryView("view_order", etlTable);

//        tableEnv.executeSql("select * from view_order").print();

        //4.创建输出表，TODO:关联到hudi表，指定hudi表名称，存储路径，字段名称等信息
        tableEnv.executeSql(
                "CREATE TABLE order_hudi_sink(\n" +
                        "    `orderId` STRING PRIMARY KEY NOT ENFORCED,\n" +
                        "    `userId` STRING,\n" +
                        "    `orderTime` STRING,\n" +
                        "    `ip` STRING,\n" +
                        "    `orderMoney` DOUBLE,\n" +
                        "    `orderStatus` INT,\n" +
                        "    `ts` STRING,\n" +
                        "    `partition_day` STRING\n" +
                        ")\n" +
                        "PARTITIONED BY (partition_day)\n" +
                        "WITH(\n" +
                        "    'connector' = 'hudi',\n" +
                        "    'path'='hdfs://cdh02:8020/user/hive/warehouse/flink_hudi_order',\n" +
                        "    'table.type' = 'MERGE_ON_READ',\n" +
                        "    'write.operation' = 'upsert',\n" +
                        "    'hoodie.datasource.write.recordkey.field' = 'orderId',\n" +
                        "    'write.precombine.field' = 'ts',\n" +
                        "    'write.tasks' = '1'\n" +
                        ")\n"
        );

//        5.通过子查询的方式，将数据写入输出表   // 插入可以在第一次的时候启动，第一次插入后，查询表数据是 空的，需要第二次启动时，注释掉插入语句，直接查询表。建表的时候会和之前的路径里的数据关联，可以直接查到
//        tableEnv.executeSql(
//                "INSERT INTO order_hudi_sink " +
//                        "SELECT orderId, userId, orderTime, ip, orderMoney, orderStatus, ts, partition_day FROM view_order"
//        );
        tableEnv.executeSql("select * from order_hudi_sink limit 20").print();
        //4.创建输出表，TODO:关联到hudi表，指定hudi表名称，存储路径，字段名称等信息
//        tableEnv.executeSql(
//                "CREATE TABLE t1(\n" +
//                        "    `uuid` STRING PRIMARY KEY NOT ENFORCED,\n" +
//                        "    `name` STRING,\n" +
//                        "    `age` INT,\n" +
//                        "    `ts` STRING,\n" +
//                        "    `partition` STRING\n" +
//                        ")\n" +
//                        "PARTITIONED BY (`partition`)\n" +
//                        "WITH(\n" +
//                        "    'connector' = 'hudi',\n" +
//                        "    'path'='hdfs://cdh02:8020/user/hive/warehouse/hudi-test-t1',\n" +
//                        "    'table.type' = 'MERGE_ON_READ',\n" +
//                        "    'write.operation' = 'upsert',\n" +
//                        "    'hoodie.datasource.write.recordkey.field' = 'orderId',\n" +
//                        "    'write.precombine.field' = 'ts',\n" +
//                        "    'write.tasks' = '1'\n" +
//                        ")\n"
//        );
//        tableEnv.executeSql("INSERT INTO t1 VALUES\n" +
//                "  ('id1','Danny',23,'1688271146','par1')");
//        tableEnv.executeSql("INSERT INTO t1 VALUES\n" +
//                "  ('id2','Stephen',33,'20230516131853008','par1')");

//        tableEnv.executeSql("select * from t1 limit 20").print();
    }
}
