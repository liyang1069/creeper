package my.creep

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.rdd.RDD

object StreamTest {
  
  def main(args: Array[String]){
    println("=================start spark==================")
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("kafka-spark-demo")
    val streamContext = new StreamingContext(sparkConf, Seconds(5))
    streamContext.checkpoint(".")
    //val topics = Set("creeper")
    val topics = Map[String, Integer]("creeper" -> 1)
    //val kafkaParam = Map[String, String]("metadata.broker.list" -> "localhost:9092") // kafka的broker list地址
    //val stream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streamContext, kafkaParam, topics)
    val stream: JavaPairReceiverInputDStream[(String, String)] = KafkaUtils.createStream(streamContext, "localhost", "", topics)
    stream.foreachRDD(rdd => {
      rdd.foreach(l => println(l))
    })
    //stream.print(10)
    stream.start()
    streamContext.awaitTermination()
  }
}