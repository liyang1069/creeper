package my.creep

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import unicredit.spark.hbase._
import com.hankcs.hanlp.HanLP

object Read extends App {
  val name = "Example of read from HBase table"

  lazy val sparkConf = new SparkConf().setAppName(name)
  lazy val sc = new SparkContext(sparkConf)
  implicit val config = HBaseConfig() // Assumes hbase-site.xml is on classpath
  val tableName = "origin_news"

  var rdd = sc.hbase[String](tableName, Set("cf1"))
    .map({ case (k, v) =>
      val cf1 = v("cf1")
      val col1 = cf1("url")
      val col2 = cf1("title")
      col2
  })
  rdd = rdd.cache()
  println("================" + rdd.count())
  for(x <- rdd.collect()){
    println(HanLP.segment(x))
  }
}