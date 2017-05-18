package my.creep

import com.hankcs.hanlp.HanLP

class Common {
  
  def html2str(htmlString: String): String={
    return htmlString.replaceAll("<style[^>]+>[^<]+</style>", "").replaceAll("<script[^>]+>[^<]+</script>", "").replaceAll("<[^>]+>", "")
  }
  
  def doc2keywords(doc: String): List[String] = {
    var segment = HanLP.segment(doc)
    var keySegment: List[String] = List()
    val equalArray = Array("b","c","cc","e","f","h","k","l","mg","Mg","mq","o","p","pba","rr","rz","ude1","vshi","vyou","vf","w","y","yg","z","zg")
    val stringArray = Array("可以","发现","可","可能","能","像","一些","些","个","没有","一定","一定会","会","比如","就是","不会","具有","有")
    val startArray = Array("a","d","p","r","u","w","y","z")
    for( s <- segment.toArray()){
      val splitArray = s.toString().split("/")
      if(equalArray.indexOf(splitArray(1)) < 0 && stringArray.indexOf(splitArray(0)) < 0){
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