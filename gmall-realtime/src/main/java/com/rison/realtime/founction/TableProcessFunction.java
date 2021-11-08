package com.rison.realtime.founction;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.rison.realtime.bean.TableProcess;
import com.rison.realtime.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private OutputTag<JSONObject> objectOutputTag;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    private Connection connection;


    public TableProcessFunction(OutputTag<JSONObject> objectOutputTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.objectOutputTag = objectOutputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void processElement(JSONObject jsonObject, ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        String key = jsonObject.getString("tableName" ) + "-" + jsonObject.getString("type");
        TableProcess tableProcess = broadcastState.get(key);
        if (tableProcess != null){
            //过滤字段
            JSONObject data = jsonObject.getJSONObject("after");
            filterColumn(data, tableProcess.getSinkColumns());

            //分流
            //将输出表/主题信息写入value
            String sinkType = tableProcess.getSinkType();
            jsonObject.put("sinkTable", sinkType);
            if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)){
                //kafka数据写入主流
                collector.collect(jsonObject);
            }else if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)){
                readOnlyContext.output(objectOutputTag, jsonObject);
            }else {
                System.out.println("该组合key:" + key + "不存在！");
            }
        }
    }

    @Override
    public void processBroadcastElement(String s, Context context, Collector<JSONObject> collector) throws Exception {
        // 获取并解析数据
        JSONObject jsonObject = JSON.parseObject(s);
        String data = jsonObject.getString("after");
        TableProcess tableProcess = JSON.parseObject(data, TableProcess.class);

        //建表
        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
            checkTable(tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend()
            );
        }

        // 写出状态，广播出去
        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSinkTable() + "-" + tableProcess.getOperateType();
        broadcastState.put(key, tableProcess);
    }

    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) throws SQLException {
        PreparedStatement preparedStatement = null;
        try{
            if (sinkPk == null) {
                sinkPk = "id";
            }
            if (sinkExtend == null) {
                sinkExtend = "";
            }
            StringBuffer createTableSQL = new StringBuffer("create table if not exists")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");

            String[] fields = sinkColumns.split(",");
            for (int i = 0; i < fields.length; i++) {
                String field = fields[i];

                //判断是否为为主键
                if (sinkPk.equals(field)) {
                    createTableSQL.append(field).append("varchar primary key");
                } else {
                    createTableSQL.append(field).append("varchar");
                }
                //判断是否为最后一个字段，如果不是，择添加"，"
                if (i < field.length() - 1) {
                    createTableSQL.append(",");
                }
            }
            createTableSQL.append(")").append(sinkExtend);

            //打印建表语句
            System.out.println(createTableSQL);

            //预编译SQL
            preparedStatement = connection.prepareStatement(createTableSQL.toString());
            preparedStatement.execute();
        }catch (SQLException e ){
            throw new RuntimeException("Phoenix表" + sinkTable + "建表失败！");
        }finally {
            if (preparedStatement != null){
                preparedStatement.close();
            }
        }
    }

    private void filterColumn(JSONObject data, String sinkColumns){
        String[] fields = sinkColumns.split(",");
        List<String> columns = Arrays.asList(",");
        data.entrySet().removeIf(next -> !columns.contains(next.getKey()));
    }

}
