package my.creep

import java.sql.DriverManager
import java.sql.ResultSet
import com.hankcs.hanlp.HanLP
import java.security.MessageDigest
import java.sql.Connection
import java.io.PrintWriter
import java.io.File

object Tesddd {
  def main(args: Array[String]): Unit = {
    val driver = "com.mysql.jdbc.Driver"
    val jdbcUrl = "jdbc:mysql://127.0.0.1/newscreeper?useUnicode=true&characterEncoding=utf-8&user=jerry&password=jerry!"
    Class.forName(driver)
    val conn = DriverManager.getConnection(jdbcUrl)
    val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
    
    writeTrainFile(conn)
    //test(conn)
    println("end")
    conn.close()
    return
    val resultSet = statement.executeQuery("select * from news where analysed = 0")
    try{
      while ( resultSet.next() ) {
        val id = resultSet.getInt("id")
        val title = resultSet.getString("title")
        val url = resultSet.getString("url")
        val classificationId = resultSet.getInt("classification_id")
        val content = resultSet.getString("content").replaceAll("<style[^>]+>[^<]+</style>", "").replaceAll("<script[^>]+>[^<]+</script>", "").replaceAll("<[^>]+>", "")
        val keyList = doc2keywords(content)
        val keyMap = list2map(keyList)
        if(id == 20){
          println(keyMap.keys)
          println(content)
        }
        println(keyMap.keys.size)
        for(k <- keyMap.keys.toList){
          val sql = "select * from keywords where name = '" + k.replaceAll("'", "\\\\'") + "'"
          val statement2 = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
          val keySet = statement2.executeQuery(sql)
          var hasKey = false
          var kid = 0
          while(keySet.next()){
            hasKey = true
            kid = keySet.getInt("id")
          }
          if(!hasKey){
            val prepKey = conn.prepareStatement("INSERT INTO keywords (name) VALUES (?) ")
            prepKey.setString(1, k)
            //println("INSERT INTO keywords "+prepKey.executeUpdate())
            prepKey.executeUpdate()
            val statement3 = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
            val keySet2 = statement3.executeQuery(sql)
            while(keySet2.next()){
              kid = keySet2.getInt("id")
            }
          }
          if(kid > 0 && classificationId > 0){
            val prepRelation = conn.prepareStatement("INSERT INTO classification_keywords (classification_id, keyword_id, news_id, amount) VALUES (?,?,?,?) ")
            prepRelation.setInt(1, classificationId)
            prepRelation.setInt(2, kid)
            prepRelation.setInt(3, id)
            prepRelation.setInt(4, keyMap.get(k).get)
            prepRelation.executeUpdate()
            //println("INSERT INTO classification_keywords "+prepRelation.executeUpdate())
          }
        }
      }
    }
    catch{
      case e: Exception => println(e.getMessage)
    }
    println("end")
    conn.close()
  }
  
  def test(conn: Connection): Unit={
    val statement = conn.createStatement()
    val rs = statement.executeQuery("select * from news where id = 11")
    var content = ""
    while(rs.next()){
      content = rs.getString("content").replaceAll("<style[^>]+>[^<]+</style>", "").replaceAll("<script[^>]+>[^<]+</script>", "").replaceAll("<[^>]+>", "")
    }
    val list = doc2keywords(content)
    println(list2map(list))
  }
  
  def writeTrainFile(conn: Connection): Unit={
    val statement = conn.createStatement()
    val rs = statement.executeQuery("select max(news_id) as news_id from classification_keywords")
    var news_id = 0
    while(rs.next()){
      news_id = rs.getInt("news_id")
    }
    val writer = new PrintWriter(new File("trainFile.txt"))
    for(index <- 1 to news_id){
      val statement2 = conn.createStatement()
      val sql = "select * from classification_keywords where news_id = " + index + " order by keyword_id asc"
      val rs2 = statement2.executeQuery(sql)
      var str = ""
      var first = true
      while(rs2.next()){
        if(first){
          first = false
          str += rs2.getInt("classification_id") + " " + rs2.getInt("keyword_id") + ":" + rs2.getInt("amount")
        }
        else{
          str += " " + rs2.getInt("keyword_id") + ":" + rs2.getInt("amount")
        }
      }
      if(str.length() > 0){
        //writer.println(str)
        writer.write(str + "\n")
      }
    }
    writer.close()
    
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