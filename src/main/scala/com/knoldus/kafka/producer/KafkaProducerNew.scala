package com.knoldus.kafka.producer

import java.util.{Properties, UUID}

import kafka.message.DefaultCompressionCodec
import org.apache.kafka.clients.producer.ProducerRecord


/**
  * Created by larryf on 9/4/2016.
  */
class KafkaProducerNew(brokerList: String, batchSize: Int, bufferSize: Int, compression: String = "none") {

  private val props = new Properties()

  props.put("bootstrap.servers", brokerList)
  props.put("retries", "0");
  props.put("batch.size", s"${batchSize}");
  props.put("linger.ms", "1");
  props.put("buffer.memory", s"${bufferSize}");
  props.put("compression.type", compression) // none, gzip, snappy, or lz4
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("client.id", UUID.randomUUID().toString())

//  props.put("compression.codec", DefaultCompressionCodec.codec.toString)
//  props.put("producer.type", "async")
//  props.put("batch.num.messages", "200")
//  props.put("message.send.max.retries", "5")
//  props.put("request.required.acks", "-1")

  val producer = new org.apache.kafka.clients.producer.KafkaProducer[String, String](props)

  def send(topic: String, message: String): Unit = {
    try {
      val record = new ProducerRecord[String, String](topic, message)
      producer.send(record)
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
    }
  }

  def close(): Unit = {
    producer.close
  }

}
