package com.atguigu.gmall.publisher.controller

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.atguigu.gmall.publisher.service.PublisherService
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.{GetMapping, RestController}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


@RestController
class PublisherController @Autowired()(val publisherService: PublisherService){


  @GetMapping(Array("/realtime-total"))
  def getRealTimeTotal(date: String) = {
    val totalDau: Long = publisherService.getDauCountByDate(date)
    /** 操他妈 scala不支持序列化 依赖又下部下来jackson的scala */
    val jSONArray: JSONArray = new JSONArray()
    val object1: JSONObject = new JSONObject()
    object1.put("id","dau")
    object1.put("name","新增日活")
    object1.put("value",totalDau)
    jSONArray.add(object1)
    val object2: JSONObject = new JSONObject()
    object2.put("id","new_mid")
    object2.put("name","新增设备")
    object2.put("value",323)
    jSONArray.add(object2)

    jSONArray.toJSONString
  }

}
