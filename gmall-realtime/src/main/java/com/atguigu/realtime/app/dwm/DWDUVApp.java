package com.atguigu.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseApp;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.MyKafkaUtil;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

public class DWDUVApp extends BaseApp {
    public static void main(String[] args) {
        new DWDUVApp().init(3001,2,"DWDUVApp","DWDUVApp", Constant.DWD_PAGE_LOG);
    }
    @Override
    protected void run(StreamExecutionEnvironment env,
                       DataStreamSource<String> sourceStream) {
        sourceStream
                .map(JSON::parseObject)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((obj,ts) -> obj.getLong("ts"))
                )
                .keyBy(obj -> obj.getJSONObject("common").getString("mid"))
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>() {


                    private SimpleDateFormat simpleDateFormat;
                    private ValueState<Long> firstVisitState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        firstVisitState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("firstVisitState", Long.class));
                        simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                    }

                    @Override
                    public void process(String key,
                                        Context ctx,
                                        Iterable<JSONObject> elements,
                                        Collector<JSONObject> out) throws Exception {
                        String current = simpleDateFormat.format(new Date(ctx.window().getEnd()));
                        if (firstVisitState.value() == null
                                || current.equals(simpleDateFormat.format(new Date(firstVisitState.value()))) ) {
                            ArrayList<JSONObject> list = Lists.newArrayList(elements);
                            JSONObject obj = Collections.min(list, Comparator.comparing((o -> o.getLong("ts"))));
                            out.collect(obj);
                            firstVisitState.update(obj.getLong("ts"));
                        }
                    }
                })
                .addSink(MyKafkaUtil.getKafkaSink(Constant.DWM_UV));
    }
}
