package my.creep

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import unicredit.spark.hbase._
import _root_.com.hankcs.hanlp.HanLP
import java.security.MessageDigest
//import com.mysql.jdbc.Driver
import java.sql.DriverManager
import java.sql.ResultSet

object Read extends App {
  val name = "Example of read from HBase table"

  lazy val sparkConf = new SparkConf().setAppName(name)
  lazy val sc = new SparkContext(sparkConf)
  implicit val config = HBaseConfig() // Assumes hbase-site.xml is on classpath
  val tableName = "origin_news"
  val driver = "com.mysql.jdbc.Driver"
  val jdbcUrl = "jdbc:mysql://123.57.247.225/teardowall?useUnicode=true&characterEncoding=utf-8&user=jerry&password=jerry!"
  Class.forName(driver)
  //classOf[com.mysql.jdbc.Driver]
  val conn = DriverManager.getConnection(jdbcUrl)
  val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
  
  var rdd = sc.hbase[String](tableName, Set("cf1"))
    .map({ case (k, v) =>
      val cf1 = v("cf1")
      val url = cf1("url")
      val title = cf1("title")
      val content = cf1("content")
      
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
    println(keywords)
    val digest = MessageDigest.getInstance("MD5")
    val md5Str = digest.digest(keywords.toString().getBytes).map("%02x".format(_)).mkString
    try{
      //statement.executeQuery("insert into creeper_news (url, title, content, md5_str) value (" + url + "," + title + "," + content + "," + md5Str + "," + ")")
      val prep = conn.prepareStatement("INSERT INTO creeper_news (url, title, content, md5_str) VALUES (?, ?, ?, ?) ")
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
}