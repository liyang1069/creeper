package my.creep

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import unicredit.spark.hbase._
import _root_.com.hankcs.hanlp.HanLP
import java.security.MessageDigest
//import com.mysql.jdbc.Driver
import java.sql.DriverManager
import java.sql.ResultSet
import org.apache.hadoop.hbase.filter.PrefixFilter

//~/spark-1.6.3-bin-hadoop2.6/bin/spark-submit --jars /Users/jerry.li/hbase-1.2.5/lib/hbase-common-1.2.5.jar,/Users/jerry.li/hbase-1.2.5/lib/hbase-client-1.2.5.jar,/Users/jerry.li/hbase-1.2.5/lib/hbase-server-1.2.5.jar,/Users/jerry.li/hbase-1.2.5/lib/hbase-protocol-1.2.5.jar,/Users/jerry.li/hbase-1.2.5/lib/hbase-hadoop-compat-1.2.5.jar,/Users/jerry.li/hbase-1.2.5/lib/htrace-core-3.1.0-incubating.jar,/Users/jerry.li/hbase-1.2.5/lib/metrics-core-2.2.0.jar --master local --class my.creep.Read target/scala-2.10/creeper-assembly-0.1.0.jar
object Read extends App {
  val name = "Example of read from HBase table"

  lazy val sparkConf = new SparkConf().setAppName(name)
  lazy val sc = new SparkContext(sparkConf)
  implicit val config = HBaseConfig() // Assumes hbase-site.xml is on classpath
  val tableName = "origin_news"
  val driver = "com.mysql.jdbc.Driver"
  val jdbcUrl = "jdbc:mysql://127.0.0.1/newscreeper?useUnicode=true&characterEncoding=utf-8&user=jerry&password=jerry!"
  Class.forName(driver)
  //classOf[com.mysql.jdbc.Driver]
  val conn = DriverManager.getConnection(jdbcUrl)
  
  //If you need to read also timestamps, you can use in both cases sc.hbaseTS[K, Q, V] and obtain a RDD[(K, Map[String, Map[Q, (V, Long)]])].
  val lastTime: Long = 1493420649711L
  var rdd = sc.hbaseTS[String](tableName, Set("cf1")).filter({ case (k, v) => v("cf1")("url")._2.>(lastTime)})
    .map({ case (k, v) =>
      val cf1 = v("cf1")
      val url = cf1("url")._1
      val title = cf1("title")._1
      val content = cf1("content")._1
      val timestamp = cf1("url")._2
      
      List(url, title, content) mkString "\t"
  })
  rdd = rdd.cache()
  println("================" + rdd.count())
  
//  for(line <- rdd.collect()){
//    write2mysql(line)
//  }
  
  rdd.foreach(line => write2mysql(line))
  conn.close()
  println("+++++++++++++++++++++++++++++++++++++++++++++++")
  
  def write2mysql(line:String):String = {
    val array = line.split("\t")
    val url = array(0)
    val title = array(1)
    var content = array(2)
    if (array.length > 3){
      var index = 0
      for(a <- array){
        if(index > 2){
          content += "\t" + a
        }
        index += 1
      }
    }
    val keywords = HanLP.extractKeyword(content.replaceAll("<[^>]+>", ""), 10)
    //println(keywords)
    val digest = MessageDigest.getInstance("MD5")
    val md5Str = digest.digest(keywords.toString().getBytes).map("%02x".format(_)).mkString
    val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
    val keyList = doc2keywords(content)
    val keyMap = list2map(keyList)
    try{
      val prep = conn.prepareStatement("INSERT INTO news (url, title, content, md5_str) VALUES (?, ?, ?, ?) ")
      prep.setString(1, url)
      prep.setString(2, title)
      prep.setString(3, content)
      prep.setString(4, md5Str)
      prep.executeUpdate
    }
    catch{
      case e: Exception => println(e.getMessage)
    }
    return md5Str
  }
  
  def doc2keywords(doc: String): List[String] = {
    var segment = HanLP.segment(doc)
    var keySegment: List[String] = List()
    val equalArray = Array("b","c","cc","e","f","h","k","l","mg","Mg","mq","o","p","pba","rr","rz","ude1","vshi","vyou","vf","w","y","yg","z","zg")
    val startArray = Array("a","d","p","r","u","w","y","z")
    for( s <- segment.toArray()){
      val splitArray = s.toString().split("/")
      if(equalArray.indexOf(splitArray(1)) < 0){
        var hasFind = false
        for(st <- startArray){
          if(splitArray(1).startsWith(st))
            hasFind = true
        }
        if(!hasFind)
          keySegment = keySegment.+:(splitArray(0))
      }
    }
    return keySegment
  }
  
  def list2map(list: List[String]): Map[String, Int] = {
    var map:Map[String,Int] = Map()
    for(s <- list){
      if(map.contains(s)){
        map = map + (s -> (map.get(s).get + 1))
      }
      else{
        map = map + (s -> 1)
      }
    }
    return map
  }
}