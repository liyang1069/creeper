package my.creep

import java.sql.DriverManager
import java.sql.ResultSet
import com.hankcs.hanlp.HanLP
import java.security.MessageDigest

object Tesddd {
  def main(args: Array[String]): Unit = {
    val driver = "com.mysql.jdbc.Driver"
    val jdbcUrl = "jdbc:mysql://127.0.0.1/newscreeper?useUnicode=true&characterEncoding=utf-8&user=jerry&password=jerry!"
    Class.forName(driver)
    val conn = DriverManager.getConnection(jdbcUrl)
    val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
    
    val resultSet = statement.executeQuery("select * from news where analysed = 0")
    while ( resultSet.next() ) {
      val id = resultSet.getString("id")
      val title = resultSet.getString("title")
      val url = resultSet.getString("url")
      val content = resultSet.getString("content").replaceAll("<[^>]+>", "")
      val keyList = doc2keywords(content)
      val keyMap = list2map(keyList)
//      try{
//        val prep = conn.prepareStatement("INSERT INTO news (url, title, content, md5_str) VALUES (?, ?, ?, ?) ")
//        prep.setString(1, url)
//        prep.setString(2, title)
//        prep.setString(3, content)
//        prep.executeUpdate
//      }
//      catch{
//        case e: Exception => println(e.getMessage)
//      }
    }
    conn.close()
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