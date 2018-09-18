package org.apache.spark.streaming.kafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset

import scala.reflect.ClassTag

/**
  * Created by Chihom on 2018/03/16.
  */

class kafkaManager(val kafkaParams: Map[String,String]) {

  private val kc = new KafkaCluster(kafkaParams)

  /**
   * @param ssc
   * @param kafkaParams
   * @param topics
   * @tparam K
   * @tparam V
   * @tparam KD
   * @tparam VD
   * @return
   */
  def createDirectStream[K: ClassTag, V: ClassTag, KD <: Decoder[K]: ClassTag, VD <: Decoder[V]: ClassTag]
  (ssc: StreamingContext, kafkaParams: Map[String, String], topics: Set[String]): InputDStream[(K, V)] = {

    val groupID = kafkaParams.get("group.id").get

    //从zookeeper上获取offsets前，必须先根据实际情况更新offsets
    setOrUpdateOffsets(topics,groupID)

    //根据zookeeper上读取offset再消费message
    val messages = {
      val partitionEn = kc.getPartitions(topics) //返回Either类型
      if (partitionEn.isLeft) throw new SparkException("Get kafka partition failed !")
      val partitions = partitionEn.right.get//返回(toipic,parttitionid)
      val consumerOffsetsEn = kc.getConsumerOffsets(groupID,partitions)
      if (consumerOffsetsEn.isLeft) throw new SparkException("Get kafka consumer offsets failed !")
      val consumerOffsets = consumerOffsetsEn.right.get//返回((topic,partitionid),offset(Long))

      KafkaUtils.createDirectStream[K, V, KD, VD, (K, V)](
      //根据实际的offset来接入数据流
      ssc, kafkaParams, consumerOffsets, (mmd: MessageAndMetadata[K, V]) => (mmd.key, mmd.message)
      )
    }
    messages
  }

  /**
   * 创建数据流之前，根据实际消费情况更新offsets：
   * 1、首次消费时，zookeeper上没有相应的group offsets目录，这时要先初始化一下zk上的offsets目录，或者是zk上记录的offsets已经过时
   * 2、由于kafka有定时清理策略，直接从zk上的offsets开始消费会报ArrayOutRange异常，即找不到offsets所属的index文件
   * @param topics
   * @param groupID
   */
  private def setOrUpdateOffsets(topics: Set[String], groupID: String): Unit = {
    topics.foreach(topic => {
      var hasConsumed = true
      val partitionsEn = kc.getPartitions(Set(topic))
      if (partitionsEn.isLeft) throw new SparkException("Get kafka partitions failed ！")
      val partitions = partitionsEn.right.get
      val consumerOffsetsEn = kc.getConsumerOffsets(groupID, partitions)
      if (consumerOffsetsEn.isLeft) hasConsumed = false
      if (hasConsumed) {//已消费(配置过时情形)
        /**
         * 如果zk上保存的offsets已经过时了，即kafka的定时清理策略已经将包含该offsets的文件删除
         * 针对这种情况，只要判断一下zk上的consumerOffsets和earliestleaderoffsets的大小
         * 如果consumeroffsets比earliestleaderoffsets还小，说明consumeroffsets已经过时。
         * 这时把consumeroffsets更新为earliestleaderoffsets
         */
        val earliestLeaderOffsets = kc.getEarliestLeaderOffsets(partitions).right.get
        val consumerOffsets = consumerOffsetsEn.right.get

        //可能只是存在部分分区consumeroffsets过时，所以只更新过时分区的consumerOffsets为earliest
        var offsets: Map[TopicAndPartition, Long] = Map()
        consumerOffsets.foreach({case(tp, n) =>
          val earliestLeaderOffset = earliestLeaderOffsets(tp).offset
          if (n < earliestLeaderOffset) {
            println("Consumer group: " + groupID + ", topic: " + tp.topic + ", partition: " + tp.partition + "offset is old ! Set the offset for "
              + earliestLeaderOffset)
            offsets += (tp -> earliestLeaderOffset)
          }
        })
        if (!offsets.isEmpty) {
          kc.setConsumerOffsets(groupID, offsets)
        }
      } else { //首次消费(或无该offsets信息)
        val reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase)
        var leaderOffsets: Map[TopicAndPartition, LeaderOffset] = null
        if (reset == Some("smallest")) {
          leaderOffsets = kc.getEarliestLeaderOffsets(partitions).right.get
        } else {
          leaderOffsets = kc.getLatestLeaderOffsets(partitions).right.get
        }
        val offsets = leaderOffsets.map {
          case(tp, offset) => (tp, offset.offset)
        }
        kc.setConsumerOffsets(groupID, offsets)
      }

    })
  }

  /**
   * 更新维护zookeeper上给定消费组的offsets
   * untilOffset尾值未消费
   * @param rdd
   */
  def updateZKOffsets(rdd: RDD[(String, String)]): Unit = {
    val groupID = kafkaParams.get("group.id").get
    val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    for (offsets <- offsetsList) {
      val topicAndPartition = TopicAndPartition(offsets.topic, offsets.partition)
      val o = kc.setConsumerOffsets(groupID,Map((topicAndPartition, offsets.untilOffset)))
      if (o.isLeft) {
        println(s"Error updating the offsets to Kafka cluster: ${o.left.get}")
      }
    }
  }

}
