package com.atguigu.gmall.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.common.constant.GmallConstant
import com.atguigu.gmall.realtime.bean.{CouponAlertInfo, EventInfo}
import com.atguigu.gmall.realtime.util.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks

object AlertApp {

  /**
   * 需求：同一设备，5分钟内三次及以上用不同账号登录并领取优惠劵，并且在登录到领劵过程中没有浏览商品。达到以上要求则产生一条预警日志。
   * 同一设备，每分钟只记录一次预警。
   *
   * 分解:
   *    1.同一个设备 --> 统一mid
   *    2.5分钟内  --> 设定窗口期
   *    3.三次不同账户 -> 窗口期内三个不同的uid
   *    4.领取优惠卷 -> 日志类型是coupon
   *    5.没有浏览商品 -> 日志类型不是clickItem的
   *
   * 每分钟只记录一次预警：
   * 言外之意就是： 产生的预警日志 应该一分钟内只有一个mid的
   * 解决方案： 采用ES的id来解决，通过mid-时间戳作为id来保证一分钟内的只会保存一条数据
   * 也可以采用redis来保存来设置ttl为1分钟，插入es前对于mid进行过滤去重操作 => redis的过期时间只能设置顶级key 这样就非常麻烦 需要写很多的代码 不推荐
   */
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AlertApp")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    val kafkaInputDstream: InputDStream[ConsumerRecord[String, String]]
                                          = MyKafkaUtil.getKafkaDStream(ssc, GmallConstant.KAFKA_TOPIC_Event)

    //讲日志数据传转成样例类
    val eventInfoDStream: DStream[EventInfo] = kafkaInputDstream.map(record => {
      val jsonStr: String = record.value()
      val eventInfo: EventInfo = JSON.parseObject(jsonStr, classOf[EventInfo])
      val dateStr: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(eventInfo.ts))
      val dateArr: Array[String] = dateStr.split(" ")
      eventInfo.logDate = dateArr(0)
      eventInfo.logHour = dateArr(1)
      eventInfo
    })

    //开窗实现5分钟内的数据分析
    val windowEventInfoDStream: DStream[EventInfo] = eventInfoDStream.window(Minutes(5), Seconds(5))

    //通过mid进行分组
    val midToEventInfoDStream: DStream[(String, Iterable[EventInfo])] =
                                            windowEventInfoDStream.map(eventInfo => (eventInfo.mid, eventInfo)).groupByKey()

    val isAlertDStream: DStream[(Boolean, CouponAlertInfo)] = midToEventInfoDStream.map { case (mid, eventInfoIter) => {
      var isAlert: Boolean = true
      //保存当前mid下有多少个用户(uid)领取过优惠卷
      val couponUidsSet: util.HashSet[String] = new util.HashSet[String]()
      //保存领取优惠卷的商品是那些
      val itemIdsSet: util.HashSet[String] = new util.HashSet[String]()
      //保存当前mid下操作事件的类型
      val eventsList: util.LinkedList[String] = new util.LinkedList[String]()
      Breaks.breakable {
        for (elem <- eventInfoIter) {
          eventsList.add(elem.evid)
          if (elem.evid == "clickItem") {
            isAlert = false
            Breaks.break()
          }
          if (elem.evid == "coupon") {
            couponUidsSet.add(elem.uid)
            itemIdsSet.add(elem.itemid)
          }
        }
      }
      if (couponUidsSet.size() < 3) isAlert = false
      (isAlert, CouponAlertInfo(mid, couponUidsSet, itemIdsSet, eventsList, System.currentTimeMillis()))
    }}

    //对不需要的预警的数据进行过滤
    val alertBeanDStream: DStream[(Boolean, CouponAlertInfo)] = isAlertDStream.filter(_._1)

    //转换数据结构
    val alertInfoWithOnlyKeyDStream: DStream[(String, CouponAlertInfo)] = alertBeanDStream.map { case (true, alertInfo) => {
      val key: String = alertInfo.mid + "_" + alertInfo.ts / 1000 / 60
      (key, alertInfo)
    }
    }

    //保存到es中，处理好key保证每分钟的mid警报的幂等性

    alertInfoWithOnlyKeyDStream.foreachRDD(rdd => {
      rdd.foreachPartition(resultIter =>
        MyEsUtil.insertBulks(resultIter.toList, GmallConstant.ES_INDEX_ALERT, GmallConstant.ES_DEFAULT_TYPE))
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
