package com.rison.realtime.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.rison.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        // todo 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //todo 设置checkopint & 状态后端
        env.setStateBackend(new FsStateBackend("hdfs://centos101:8020/gmall-flink-210325/ck"));
        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointTimeout(1000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(3);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));
        //todo 消费obs_base_log 主题数据创建流
        String sourceTopic = "ods_base_log";
        String groupId = "base_log_app";
        DataStreamSource<String> steamSource = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));
        //todo 将数据转换为JSON对象
        OutputTag<String> outPutTag = new OutputTag<String>("Dirty");
        SingleOutputStreamOperator<JSONObject> jsonObjectDataStream = steamSource.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String s, Context context, Collector<JSONObject> collector) throws Exception {
                        try {
                            JSONObject jsonObject = JSON.parseObject(s);
                            collector.collect(jsonObject);
                        } catch (Exception e) {
                            //json 解析异常，放到脏数据测输出流
                            context.output(outPutTag, s);
                        }
                    }
                }
        );

        //todo  打印脏数据
        jsonObjectDataStream.getSideOutput(outPutTag).print();
        //todo 新老用户校验 状态编程
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDateStream = jsonObjectDataStream.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"))
                .map(new RichMapFunction<JSONObject, JSONObject>() {
                    private ValueState<String> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        valueState = this.getRuntimeContext().getState(new ValueStateDescriptor<String>("value-state", String.class));
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        // 获取数据中的"is_new"标记
                        String isNew = jsonObject.getJSONObject("common").getString("is_new");
                        if ("1".equals(isNew)) {
                            //获取状态数据
                            String state = valueState.value();
                            if (state != null) {
                                jsonObject.getJSONObject("common").put("is_new", "0");
                            } else {
                                valueState.update("1");
                            }
                        }
                        return jsonObject;
                    }
                });
        //todo 分流 测输出流 页面：主流   启动：测输出流   曝光： 测输出流
        OutputTag<String> startTag = new OutputTag<String>("start");
        OutputTag<String> displayTag = new OutputTag<>("display");

        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDateStream.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<String> collector) throws Exception {
                //获取启动日志
                String start = jsonObject.getString("start");
                if (start != null && start.length() > 0) {
                    //将启动日志数据写到侧输出流
                    context.output(startTag, jsonObject.toJSONString());
                } else {
                    //将页面数据写到主流
                    collector.collect(jsonObject.toJSONString());
                    //取出曝光日志数据
                    JSONArray displays = jsonObject.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        //获取页面ID
                        String pageId = jsonObject.getJSONObject("page").getString("page_id");

                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            //添加页面ID
                            display.put("page_id", pageId);
                            context.output(displayTag, display.toJSONString());
                        }
                    }
                }
            }
        });

        //todo 提取侧输出流
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);

        //todo 将三个流打印并sink到kafka
        pageDS.print();
        startDS.print();
        displayDS.print();

        startDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_start_log"));
        pageDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_page_log"));
        displayDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_display_log"));

        //todo 执行
        env.execute("base-log");
    }
}
