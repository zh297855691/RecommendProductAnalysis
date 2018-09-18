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

    //��zookeeper�ϻ�ȡoffsetsǰ�������ȸ���ʵ���������offsets
    setOrUpdateOffsets(topics,groupID)

    //����zookeeper�϶�ȡoffset������message
    val messages = {
      val partitionEn = kc.getPartitions(topics) //����Either����
      if (partitionEn.isLeft) throw new SparkException("Get kafka partition failed !")
      val partitions = partitionEn.right.get//����(toipic,parttitionid)
      val consumerOffsetsEn = kc.getConsumerOffsets(groupID,partitions)
      if (consumerOffsetsEn.isLeft) throw new SparkException("Get kafka consumer offsets failed !")
      val consumerOffsets = consumerOffsetsEn.right.get//����((topic,partitionid),offset(Long))

      KafkaUtils.createDirectStream[K, V, KD, VD, (K, V)](
      //����ʵ�ʵ�offset������������
      ssc, kafkaParams, consumerOffsets, (mmd: MessageAndMetadata[K, V]) => (mmd.key, mmd.message)
      )
    }
    messages
  }

  /**
   * ����������֮ǰ������ʵ�������������offsets��
   * 1���״�����ʱ��zookeeper��û����Ӧ��group offsetsĿ¼����ʱҪ�ȳ�ʼ��һ��zk�ϵ�offsetsĿ¼��������zk�ϼ�¼��offsets�Ѿ���ʱ
   * 2������kafka�ж�ʱ������ԣ�ֱ�Ӵ�zk�ϵ�offsets��ʼ���ѻᱨArrayOutRange�쳣�����Ҳ���offsets������index�ļ�
   * @param topics
   * @param groupID
   */
  private def setOrUpdateOffsets(topics: Set[String], groupID: String): Unit = {
    topics.foreach(topic => {
      var hasConsumed = true
      val partitionsEn = kc.getPartitions(Set(topic))
      if (partitionsEn.isLeft) throw new SparkException("Get kafka partitions failed ��")
      val partitions = partitionsEn.right.get
      val consumerOffsetsEn = kc.getConsumerOffsets(groupID, partitions)
      if (consumerOffsetsEn.isLeft) hasConsumed = false
      if (hasConsumed) {//������(���ù�ʱ����)
        /**
         * ���zk�ϱ����offsets�Ѿ���ʱ�ˣ���kafka�Ķ�ʱ��������Ѿ���������offsets���ļ�ɾ��
         * ������������ֻҪ�ж�һ��zk�ϵ�consumerOffsets��earliestleaderoffsets�Ĵ�С
         * ���consumeroffsets��earliestleaderoffsets��С��˵��consumeroffsets�Ѿ���ʱ��
         * ��ʱ��consumeroffsets����Ϊearliestleaderoffsets
         */
        val earliestLeaderOffsets = kc.getEarliestLeaderOffsets(partitions).right.get
        val consumerOffsets = consumerOffsetsEn.right.get

        //����ֻ�Ǵ��ڲ��ַ���consumeroffsets��ʱ������ֻ���¹�ʱ������consumerOffsetsΪearliest
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
      } else { //�״�����(���޸�offsets��Ϣ)
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
   * ����ά��zookeeper�ϸ����������offsets
   * untilOffsetβֵδ����
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
