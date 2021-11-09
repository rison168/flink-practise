package com.rison.realtime.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.rison.realtime.bean.TableProcess;
import com.rison.realtime.founction.CustomerDeserialization;
import com.rison.realtime.founction.TableProcessFunction;
import com.rison.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import sun.tools.jconsole.Tab;

public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        //TODO 获取执行环境

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //TODO 消费kafka ods_base_db 主题数据创建流
        String sourceTopic = "ods_base_db";
        String groupId = "base_db_app";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));
        //TODO 将每行数据转换为JSON 对象并过滤（delete） 主流
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject)
                .filter(
                        new FilterFunction<JSONObject>() {
                            @Override
                            public boolean filter(JSONObject jsonObject) throws Exception {
                                //取出数据的操作类型
                                String type = jsonObject.getString("type");

                                return !"delete".equals(type);
                            }
                        }
                );

        //TODO 使用Flink CDC 消费配置表并处理成 广播流
        DebeziumSourceFunction<String> build = MySQLSource.<String>builder()
                .hostname("centos101")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall-realtime")
                .tableList("gmall-realtime.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new CustomerDeserialization())
                .build();
        DataStreamSource<String> tableProcessDS = env.addSource(build);
        final MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<String, TableProcess>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = tableProcessDS.broadcast(mapStateDescriptor);

        //TODO 连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonObjDS.connect(broadcastStream);
        //TODO  分流 处理数据 广播流数据（根据广播流数据进行处理）
        OutputTag<JSONObject> hbaseTag = new OutputTag<>("hbase-tag");
        SingleOutputStreamOperator<JSONObject> process = connectedStream.process(new TableProcessFunction(hbaseTag, mapStateDescriptor));

        env.execute("BaseDBApp");

    }
}
