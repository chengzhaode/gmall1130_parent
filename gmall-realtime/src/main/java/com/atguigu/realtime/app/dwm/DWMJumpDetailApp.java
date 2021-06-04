package com.atguigu.realtime.app.dwm;

import com.atguigu.realtime.app.BaseApp;
import com.atguigu.realtime.common.Constant;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DWMJumpDetailApp extends BaseApp {
    public static void main(String[] args) {
        // 最终的目的: 是找到跳出的记录, 最后写入到Kafka
        new DWMJumpDetailApp().init(3002, 2, "DWMJumpDetailApp", "DWMJumpDetailApp", Constant.DWD_PAGE_LOG);
    }

    @Override
    protected void run(StreamExecutionEnvironment env,
                       DataStreamSource<String> sourceStream) {
        sourceStream =
                env.fromElements(
                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"other\"},\"ts\":20000} ",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000}",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                                "\"home\"},\"ts\":39999} ",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                                "\"detail\"},\"ts\":50000} "
                );
        // 1. 先有流   使用事件时间
        // 2. 定义模式
        // 3. 把模式运用在流上
        // 4. 从匹配到的模式流中取出需要的数据
    }
}

