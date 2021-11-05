package com.rison.realtime.ods;


import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import com.rison.realtime.founction.CustomerDeserialization;
import com.rison.realtime.utils.MyKafkaUtil;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDC {
    public static void main(String[] args) throws Exception {
        //todo 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        //设置 checkopint & 状态后端
        env.setStateBackend(new FsStateBackend("hdfs://centos01:8020/gamll-realtime"));
        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);

        //todo 2. 通过FlinkCDC构建sourceFunction 并读取数据
        DebeziumSourceFunction<String> source = MySQLSource.<String>builder()
                .hostname("centos101")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("gmall-flink")
                .deserializer(new CustomerDeserialization())
                .startupOptions(StartupOptions.latest())
                .build();

        DataStreamSource<String> steamSource = env.addSource(source);

        //todo 3. 打印数据并将数据写到kafka

        steamSource.print();
        String sinkTopic = "ods_base_db";
        steamSource.addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));

        //todo 4. 启动任务
        env.execute("flink-cdc");
    }
}
