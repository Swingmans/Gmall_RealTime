package com.atguigu.gmall.logger.controller

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.common.constant.GmallConstant
import org.slf4j.{Logger, LoggerFactory}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.web.bind.annotation.{PostMapping, RestController}

@RestController
class LoggerController @Autowired()
(private val kafkaTemplate: KafkaTemplate[String, String]) {

  private val logger: Logger = LoggerFactory.getLogger(classOf[LoggerController])


  @PostMapping(Array("/log"))
  def doLog(logString: String): String = {
    val jSONObject: JSONObject = JSON.parseObject(logString)
    jSONObject.put("ts", System.currentTimeMillis())

    //落盘保存
    logger.info(jSONObject.toJSONString)

    //推送kafka日志消息
    if ("startup" == jSONObject.get("type")) {
      kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_STARTUP, jSONObject.toJSONString)
    } else {
      kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_Event,jSONObject.toJSONString)
    }

    logString
  }

}
