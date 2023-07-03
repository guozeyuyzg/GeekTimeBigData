package hudi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink2Hudi2Hive {
    public static void main(String[] args) throws Exception {
        //1.获取表的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //并行度设置为1
        env.setParallelism(1);
        //TODO: 由于增量将数据写入到Hudi表，所以需要启动Flink Checkpoint 检查点
        env.enableCheckpointing(5 * 1000);
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()//设置流式模式
                .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);

        //查询到的数据进行提取出来，转换成为DataStream进行打印
        //   DataStream<Tuple2<Boolean, UserBean>> tuple2DataStream = tableEnvironment.toRetractStream(result, UserBean.class);
        //  tuple2DataStream.print();

        String mysql_binlog = "CREATE TABLE mysql_binlog \n" +
                "( id INT ,   name STRING,  age INT ,birthday STRING )\n" +
                " WITH (  \n" +
                " 'connector' = 'mysql-cdc' ,\n" +
                " 'hostname' = 'localhost' ,\n" +
                " 'port' = '3306' ,\n" +
                " 'username' = 'root' ,\n" +
                " 'password' = 'root' ,\n" +
                " 'database-name' = 'test_cdc' ,\n" +
                " 'table-name' = 'mysql_binlog','scan.incremental.snapshot.enabled'='false' \n" +
                " )";
        tableEnvironment.executeSql(mysql_binlog);
//        tableEnvironment.executeSql("select * from mysql_binlog").print();


        String ods_mysql_binlog_kafka = "CREATE TABLE  if not exists  ods_mysql_binlog_kafka( \n" +
                "`id` INT ,   \n" +
                "`name` STRING,  \n" +
                "`age` INT , \n" +
                "`birthday` STRING \n" +
                ") WITH(  \n" +
                "'connector' = 'kafka',  \n" +
                "'topic'='ods_mysql_binlog_kafka',  \n" +
                "'properties.bootstrap.servers' = 'cdh06:9092,cdh07:9092,cdh08:9092',  \n" +
                "'properties.group.id' = 'ods_mysql_binlog_group_consum',  \n" +
                "'scan.startup.mode' = 'earliest-offset',  \n" +
                "'format' = 'debezium-json' ,\n" +  //changelog-json
                "  'debezium-json.ignore-parse-errors'='true' \n" +
                ")";
        tableEnvironment.executeSql(ods_mysql_binlog_kafka);

        String insertmysql2kafka = "insert into ods_mysql_binlog_kafka select * from mysql_binlog";
        tableEnvironment.executeSql(insertmysql2kafka);
//        tableEnvironment.executeSql("select * from ods_mysql_binlog_kafka").print();

        String ods_mysql_binlog_hudi = "CREATE TABLE ods_mysql_binlog_hudi(  \n" +
                "id int ,  \n" +
                " name string,  \n" +
                "age int,  \n" +
                "birthday string ,  \n" +
                "ts TIMESTAMP(3),  \n" +
                "partition_day string ,  \n" +
                "primary key(id) not enforced  \n" +
                ")  \n" +
                "PARTITIONED BY (partition_day)  \n" +
                "with(  \n" +
                "'connector'='hudi',  \n" +
                "'path'= 'hdfs://cdh02:8020/user/hive/warehouse/flink_cdc_sink_hudi_hive',   \n" +
                "'table.type'= 'MERGE_ON_READ',  \n" +
                "'hoodie.datasource.write.recordkey.field'= 'id',   \n" +
                "'write.precombine.field'= 'ts',  \n" +
                "'write.tasks'= '1',  \n" +
                "'write.rate.limit'= '2000',   \n" +
                "'compaction.tasks'= '1',   \n" +
                "'compaction.async.enabled'= 'true',  \n" +
                "'compaction.trigger.strategy'= 'num_commits',  \n" +
                "'compaction.delta_commits'= '1',  \n" +
                "'changelog.enabled'= 'true',  \n" +
                "'read.streaming.enabled'= 'true',  \n" +
                "'read.streaming.check-interval'= '3'  \n" +

//                ",'hive_sync.enable'= 'true',\n" +
//                "'hive_sync.mode'= 'jdbc',\n" +  // jdbc  hms
//                "'hive_sync.metastore.uris'= 'thrift://cdh03:9083',\n" +
//                "'hive_sync.jdbc_url'= 'jdbc:hive2://cdh03:10000',\n" +
//                "'hive_sync.table'= 'flink_cdc_sink_hudi_hive',\n" +
//                "'hive_sync.db'= 'default',\n" +
//                "'hive_sync.username'= 'root',\n" +
//                "'hive_sync.password'= 'xgg@svnet', \n" +
//                "'hive_sync.skip_ro_suffix' = 'true'  ,\n" +//去除ro后缀
//                "'hive_sync.support_timestamp'= 'true'\n" +
                ");";
        tableEnvironment.executeSql(ods_mysql_binlog_hudi);

        String insertkafka2hudi = "insert into ods_mysql_binlog_hudi  \n" +
                "select id, \n" +
                "name, \n" +
                "age , \n" +
                "birthday, \n" +
                "cast(birthday as timestamp(3)) as ts , \n" +
                "substring(birthday,0,10) as partition_day  \n" +
                "from ods_mysql_binlog_kafka";


        tableEnvironment.executeSql(insertkafka2hudi);
        tableEnvironment.executeSql("select * from ods_mysql_binlog_hudi limit 20").print();

        env.execute();

    }
}