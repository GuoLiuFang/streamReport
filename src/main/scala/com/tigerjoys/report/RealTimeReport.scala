package com.tigerjoys.report

import org.apache.spark.streaming.kafka._
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.broadcast.Broadcast
import com.tigerjoys.db.DB
import org.apache.logging.log4j.LogManager
import com.tigerjoys.processor.Processor

object RealTimeReport {
  val logger = LogManager.getLogger()
  var broadcastVar = None: Option[Broadcast[Map[String, Int]]]

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Scala")
    val ssc = new StreamingContext(conf, Seconds(10))
    val lines = ssc.textFileStream("/Users/guoliufang/TestWorkSpace/streamData")
//  val kafkaStream = KafkaUtils.createStream(streamingContext, [zookeeperQuorum], [group id of the consumer], [per-topic number of Kafka partitions to consume])
    if (broadcastVar.isEmpty) {
      val db = new DB
      broadcastVar = Some(ssc.sparkContext.broadcast(db.getBroadcast))
      logger.info("广播变量加载成功")
    }
    val preProcessedLine = lines.map(line => Processor.clean(line, broadcastVar))
    

    preProcessedLine.print();
    preProcessedLine.saveAsTextFiles("/Users/guoliufang/TestWorkSpace/streamResult/" + "clean-line", "txt")
    
    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate

  } //main


  

}