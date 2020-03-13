package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.common.constant.GmallConstant
import com.atguigu.gmall.realtime.bean.OrderInfo
import com.atguigu.gmall.realtime.util.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

/**
 * 采集业务数据：订单
 * 利用canal实时读取mysql的binlog文件并缓存到canal自身的队列中
 * 编写canal客户端程序 实时批次拉取sql操作的结果集并进行ETL非ROWDATA和非INSERT操作的过滤掉 然后根据ENTRY的表明实时推送到KAFKA集群中
 * 编写SparkStreaming程序，流式消费kafka中的数据，并对数据进行封装类进行脱敏操作 实时通过phoenix写入HBase
 */
object OrderApp {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("OrderApp").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(5))

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaDStream(ssc,GmallConstant.KAFKA_TOPIC_ORDER)

    val orderInfoDStream: DStream[OrderInfo] = kafkaDStream.map(record => {
      val rowJson: String = record.value()
      val orderInfo: OrderInfo = JSON.parseObject(rowJson, classOf[OrderInfo])
      val timeArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = timeArr(0)
      orderInfo.create_hour = timeArr(1).split(":")(0)
      val telArr: Array[String] = orderInfo.consignee_tel.split("")
      orderInfo.consignee_tel = telArr.head + telArr.tail.head + telArr.tail.tail.head + "****" + telArr(7) + telArr(8) + telArr(9) + telArr.last
      orderInfo
    })

    orderInfoDStream.cache()

    //通过phoenix保存到hbase
    orderInfoDStream.foreachRDD(rdd => {
      rdd.saveToPhoenix(
        "GMALL2019_ORDER_INFO",
        Seq("ID","PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID","IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME","OPERATE_TIME","TRACKING_NO","PARENT_ORDER_ID","OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
        new Configuration(),
        Some("hadoop102,hadoop103,hadoop104:2181")
      )
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
