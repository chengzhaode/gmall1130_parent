package com.atguigu.realtime.sink;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.bean.TableProcess;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MyPhoenixSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcess>> {

    private Connection conn;
    private ValueState<Boolean> tableCreateState;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        String phoenixDriver = "org.apache.phoenix.jdbc.PhoenixDriver";
        String phoenixUrl = "jdbc:phoenix:hadoop162,hadoop163,hadoop164:2181";
        Class.forName(phoenixDriver);
        conn = DriverManager.getConnection(phoenixUrl);
        tableCreateState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("tableCreateState", Boolean.class));
    }

    @Override
    public void invoke(Tuple2<JSONObject, TableProcess> value,
                       Context context) throws Exception {
        // 每执行一次, 通过jdbc操作, 把这条数据写入到 Phoenix 中
        //1. 动态的建表, 先判断表是否存在, 如果存在则不创建这个表, 否则再去创建
        checkTable(value);  // 对表是否创建进行检测
        // 2. 把数据写入到Hbase中
        write2Hbase(value);  // 2 two to 4 four for  i18u 国际化

    }

    private void write2Hbase(Tuple2<JSONObject, TableProcess> data) throws SQLException {
        TableProcess tp = data.f1;
        JSONObject obj = data.f0;
        StringBuffer sql = new StringBuffer();
        sql
                .append("upsert into")
                .append(tp.getSinkTable())
                .append("(");
        for (String c : tp.getSinkColumns().split(",")) {
            sql.append(c).append(",");

        }
        sql.deleteCharAt(sql.length() - 1);  // 删除最后一个多余的引号

        sql.append(") values(");
        for (String c : tp.getSinkColumns().split(",")) {
            sql.append("?,");
        }
        sql.deleteCharAt(sql.length() - 1);
        sql.append(")");

        PreparedStatement ps = conn.prepareStatement(sql.toString());

        // 给占位符赋值
        String[] cs = tp.getSinkColumns().split(",");
        for (int i = 0, len = cs.length; i < len; i++) {
            String v = obj.get(cs[i]) == null ? null: obj.get(cs[i]).toString();  // null -> "null"  ""
            ps.setObject(i + 1, v);
        }

        ps.execute();
        conn.commit();
        ps.close();
    }

    private void checkTable(Tuple2<JSONObject, TableProcess> value) throws SQLException, IOException {
        if (tableCreateState.value() == null) {
            // create table user(id varchar, name varchar , constraint pk primary key(id, name))  SALT_BUCKETS = 3
            // 盐表
            TableProcess tp = value.f1;

            StringBuilder createTableSql = new StringBuilder();
            createTableSql
                    .append("create table if not exists ")
                    .append(tp.getSinkTable())
                    .append("(");

            for (String c : tp.getSinkColumns().split(",")) {
                createTableSql.append(c).append(" varchar,");
            }

            createTableSql
                    .append("constraint pk primary key(")
                    .append(tp.getSinkPk() == null ? "id" : tp.getSinkPk())
                    .append("))")
                    .append(tp.getSinkExtend() == null ? "" : tp.getSinkExtend());

            PreparedStatement ps = conn.prepareStatement(createTableSql.toString());
            ps.execute();
            conn.commit();
            ps.close();

            tableCreateState.update(true);
        }
    }

    @Override
    public void close() throws Exception {
        // 关闭连接, 释放资源
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }
}
