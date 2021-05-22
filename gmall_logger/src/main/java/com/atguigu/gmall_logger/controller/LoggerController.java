package com.atguigu.gmall_logger.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class LoggerController {
    @RequestMapping("applog")
    public String log(@RequestParam("param") String logString){
        // 1.日志落盘
        saveToDisk(logString);
        // 2. 数据写入到kafka
        sendToKafka(logString);

        return "ok";
    }
    @Autowired
    private KafkaTemplate<String, String> kafka;

    private void sendToKafka(String logString) {
        kafka.send("ods_log", logString);
    }

    private void saveToDisk(String logString) {
        log.info(logString);
    }

}

