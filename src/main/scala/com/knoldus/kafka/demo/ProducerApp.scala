package com.knoldus.kafka.demo

import com.knoldus.kafka.producer.{AsyncProducer, KafkaProducer, KafkaProducerNew}

import scala.util.Random


object ProducerApp extends App {

  val topic = "demo-topic"

  val msgSize: Int = (1024 * .25).toInt
  val batchSize = 100
  val bufferMemory = 16 * 1024 * 1024
  val numTransactions = 1000000
  val compression = "none" // none, gzip, snappy, or lz4
  val rString = Random.alphanumeric.take(msgSize).mkString

  //val producer = new AsyncProducer("kafka01:9092,kafka02:9092,kafka03:2092")
  //val producer = new KafkaProducer("localhost:9092")
  val producer = new KafkaProducerNew("kafka01:9092,kafka02:9092,kafka03:2092", batchSize, bufferMemory, compression)

  val t0 = System.nanoTime()
  (1 to numTransactions).toList.map(no => s"Msg # ${no} ${rString}")
//    .grouped(batchSize)
    .foreach { message =>
    //    println("Sending message batch size " + message.length)
//    print('.')
//    println(message)
      producer.send(topic, message)
  }

  val t1 = System.nanoTime()
  val secs = (t1 - t0) / 1E9
  val t = (t1 - t0)
  println
  println(s"Sent a total of ${numTransactions} messages (size=${msgSize}, batch=${batchSize}, buffer=${bufferMemory}, compression=${compression}) in ${secs} seconds")
  println(s"${numTransactions / secs} transactions per sec")
  producer.close()
}
