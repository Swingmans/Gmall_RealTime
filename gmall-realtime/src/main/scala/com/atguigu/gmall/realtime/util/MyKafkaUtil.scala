package com.atguigu.gmall.realtime.util

import java.util.Properties

import com.atguigu.gmall.common.constant.GmallConstant
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}


object MyKafkaUtil {

  private val prop: Properties = PropertiesUtil.getProperties(GmallConstant.GMALL_REALTIME_CONFIG)

  /** kafka集群节点 */
  private val kafkaBrokers: String = prop.getProperty("kafka.broker.list")

  /** 当前消费者组 */
  private val consumerGroup: String = "gmall_consumer_group"

  /** 重新获取offset的策略  latest-> 代表时刻都从最新获取 */
  private val offsetResetConfig: String = "latest"

  private val kafkaParams: Map[String, Object] = Map(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafkaBrokers,
    ConsumerConfig.GROUP_ID_CONFIG -> consumerGroup,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> offsetResetConfig,
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (true: java.lang.Boolean)
  )

  /**
   * 获取kafka的离散化流
   * @param ssc sparkStreaming上下文对象
   * @param topic 消费的topic
   * @return InputDStream
   */
  def getKafkaDStream(ssc: StreamingContext, topic: String): InputDStream[ConsumerRecord[String, String]] = {
    KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent, //每个Executor分配消费kafka分区的策略
      ConsumerStrategies.Subscribe[String, String](Set(topic), kafkaParams))
  }
}
