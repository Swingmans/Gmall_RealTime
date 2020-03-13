package com.atguigu.gmall.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.common.constant.GmallConstant
import com.atguigu.gmall.realtime.bean.StartUpLog
import com.atguigu.gmall.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.{Jedis, Pipeline}
import org.apache.phoenix.spark._

/**
 * 实时统计每日活跃用户数(实时统计日活设备id)
 */
object DauApp {

  def main(args: Array[String]): Unit = {

    //创建SparkStreaming上下文对象
    val conf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]]
                              = MyKafkaUtil.getKafkaDStream(ssc,GmallConstant.KAFKA_TOPIC_STARTUP)

    //转换json到样例类 并对时间字段进行处理
    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.map(record => {
      val jsonStr: String = record.value()

      val startUpLOgObject: JSONObject = JSON.parseObject(jsonStr)
      val startUpLog: StartUpLog = JSON.parseObject[StartUpLog](jsonStr, classOf[StartUpLog])
      startUpLog.logType = startUpLOgObject.getString("type")

      val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
      val dateHourStr: String = sdf.format(new Date(startUpLog.ts))
      val dateHourArr: Array[String] = dateHourStr.split(" ")

      startUpLog.logDate = dateHourArr(0)
      startUpLog.logHour = dateHourArr(1)
      startUpLog
    })

    //缓存流，减少流的压力
    startUpLogDStream.cache()

    val cycleFilterDStream: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {
      //周期性的获取redis中的mid集合并广播变量到各个executor
      val jedis: Jedis = RedisUtil.getClient
      val nowStr: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
      val midSet: util.Set[String] = jedis.smembers(GmallConstant.REALTIME_DAU_KEY_PREFIX + nowStr)
      val midCast: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(midSet)
      jedis.close()

      //对每一批次的数据过滤去重，去除已经存在的数据
      rdd.filter(startUpLog => {
        val midSet: util.Set[String] = midCast.value
        !midSet.contains(startUpLog.mid)
      })

    })
    //周期性过滤只能过滤掉已经保存在Redis中的老数据，如果新批次中存在新mid并且多个 是无法被过滤的
    //因此 进行离散化流内部过滤
    val midToStartUpLogDStream: DStream[(String, Iterable[StartUpLog])] = cycleFilterDStream
                                                                          .map(startUpLog => (startUpLog.mid, startUpLog))
                                                                          .groupByKey()
    //完成内部去重过滤
    val innerFilterDStream: DStream[StartUpLog] =
      midToStartUpLogDStream.map { case (mid, startUpLogIter) => {
      startUpLogIter.toList.sortWith(_.ts < _.ts).head
    }}


    //保存到redis中
    innerFilterDStream.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        val jedis: Jedis = RedisUtil.getClient
        //使用pipeLine减少网路io  -- 业务时批量写入redis 使用pipeline
        val pipeline: Pipeline = jedis.pipelined()
        iter.foreach(startUpLog => {
          var key: String = GmallConstant.REALTIME_DAU_KEY_PREFIX + startUpLog.logDate
          print(startUpLog.mid)
          pipeline.sadd(key,startUpLog.mid)
        })
        pipeline.sync()
        jedis.close()
      })
    })

    //保存到HBase中
    innerFilterDStream.foreachRDD(rdd => {
      rdd.saveToPhoenix(
        "GMALL2019_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        new Configuration(),
        Some("hadoop102,hadoop103,hadoop104:2181")
      )
    })


    //启动数据采集器
    ssc.start()
    //同步等待采集器接受在关闭spark程序
    ssc.awaitTermination()
  }

}
