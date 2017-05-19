package my.creep

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import unicredit.spark.hbase._
import _root_.com.hankcs.hanlp.HanLP
import java.security.MessageDigest
import java.sql.DriverManager
import java.sql.ResultSet
import org.apache.hadoop.hbase.filter.PrefixFilter
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import java.util.Date

//~/spark-1.6.3-bin-hadoop2.6/bin/spark-submit --jars /Users/jerry.li/hbase-1.2.5/lib/hbase-common-1.2.5.jar,/Users/jerry.li/hbase-1.2.5/lib/hbase-client-1.2.5.jar,/Users/jerry.li/hbase-1.2.5/lib/hbase-server-1.2.5.jar,/Users/jerry.li/hbase-1.2.5/lib/hbase-protocol-1.2.5.jar,/Users/jerry.li/hbase-1.2.5/lib/hbase-hadoop-compat-1.2.5.jar,/Users/jerry.li/hbase-1.2.5/lib/htrace-core-3.1.0-incubating.jar,/Users/jerry.li/hbase-1.2.5/lib/metrics-core-2.2.0.jar --master local --class my.creep.Read target/scala-2.10/creeper-assembly-0.1.0.jar
object Read extends App {
  val name = "Example of read from HBase table"

  lazy val sparkConf = new SparkConf().setAppName(name)
  lazy val sc = new SparkContext(sparkConf)
  implicit val config = HBaseConfig() // Assumes hbase-site.xml is on classpath
  val hbaseTableName = "origin_news"
  val driver = "com.mysql.jdbc.Driver"
  val jdbcUrl = "jdbc:mysql://127.0.0.1/newscreeper?useUnicode=true&characterEncoding=utf-8&user=jerry&password=jerry!"
  
  val common = new Common()
  
  //mysql connection
  Class.forName(driver)
  val conn = DriverManager.getConnection(jdbcUrl)
  
  //trainning data, read mysql and then write in file
  val training = MLUtils.loadLibSVMFile(sc, "/newscreeper/trainFile.txt")
  val model = NaiveBayes.train(training)//, lambda = 1.0, modelType = "multinomial")
  val broadModel = sc.broadcast(model)
  
  val lastTime: Long = getLastTime()
  val nowTime: Long = new Date().getTime
  
  //If you need to read also timestamps, you can use in both cases sc.hbaseTS[K, Q, V] and obtain a RDD[(K, Map[String, Map[Q, (V, Long)]])].
  var rdd = sc.hbaseTS[String](hbaseTableName, Set("cf1")).filter({ case (k, v) => v("cf1")("url")._2.>(lastTime)})
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
  
  rdd.foreach(line => write2mysql(line))
  setNowTime()
  conn.close()
  println("+++++++++++++++++++++++++++++++++++++++++++++++")
  
  def getLastTime(): Long={
    var time: Long = 0L
    val statement = conn.createStatement()
    val rs = statement.executeQuery("select * from common_data where description = \"lastTime\" limit 1")
    while(rs.next()){
      time = rs.getLong("data")
    }
    return time
  }
  
  def setNowTime(): Unit={
    try{
      val sql = "UPDATE common_data SET data = ? WHERE description = ?"  
      val pstm = conn.prepareStatement(sql)  
      pstm.setObject(1, nowTime)  
      pstm.setString(2, "lastTime")  
  
      pstm.executeUpdate()
    }
    catch{
      case e: Exception => println(e.getMessage)
    }

  }
  
  def getMaxKeyword(): Int={
    var maxId: Int = 0
    val statement = conn.createStatement()
    val rs = statement.executeQuery("select max(id) as id from keywords")
    while(rs.next()){
      maxId = rs.getInt("id")
    }
    return maxId
  }
  
  def write2mysql(line:String): String= {
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
    //keyword to md5, unique one article
    val keywords = HanLP.extractKeyword(common.html2str(content), 10)
    val digest = MessageDigest.getInstance("MD5")
    val md5Str = digest.digest(keywords.toString().getBytes).map("%02x".format(_)).mkString
    
    val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
    val keyList = common.doc2keywords(common.html2str(content))
    val keyMap = common.list2map(keyList)
    
    val statement1 = conn.createStatement()
    val sql = "select * from keywords where name in (\"" + keyList.mkString("\",\"") + "\") order by id asc"
    val rs1 = statement1.executeQuery(sql)
    var indexList: List[Int] = List()
    var valueList: List[Double] = List()
    println(keyMap.keys)
    while(rs1.next()){
      val kid = rs1.getInt("id")
      val kname = rs1.getString("name")
      indexList = indexList.+:(kid - 1)
      if(keyMap.get(kname) != null){
        valueList = valueList.+:(keyMap.get(kname).get.toDouble)
      }
      else{
        println("========================" + kname)
      }
    }
    val vector = Vectors.sparse(getMaxKeyword(), indexList.toArray, valueList.toArray)
    val label = broadModel.value.predict(vector)
    println("========================" + label)
    
    try{
      val prep = conn.prepareStatement("INSERT INTO news (url, title, content, md5_str,classification_id) VALUES (?, ?, ?, ?, ?) ")
      prep.setString(1, url)
      prep.setString(2, title)
      prep.setString(3, content)
      prep.setString(4, md5Str)
      prep.setDouble(5, label)
      prep.executeUpdate
    }
    catch{
      case e: Exception => println(e.getMessage)
    }
    return md5Str
  }
}