package my.creep

import scala.io.Source

object DebugClass {
  def main(args: Array[String]): Unit = {
    val common = new Common()
    var str = ""
    for(line<-Source.fromFile("debug.txt").getLines)
      str += line
    
    var tests = str
    println(tests.replaceAll("<script[^>]*?>.*?</script>", "").replaceAll("<style[^>]*?>.*?</style>", "").replaceAll("<[^>]*>", ""))
    //println(str)
    //str = "<div ><script>!function t(i,e,o){fu</script>sdaf<script type=\"text/javascript\">    var related_video_info = {  </script> </div>"
    //..replaceAll("<style></style>", "").replaceAll("<script></script>", "").replaceAll("<style>[^<]+</style>", "").replaceAll("<script>[^<]+</script>", "").replaceAll("<style[^>]+>[^<]+</style>", "").replaceAll("<script[^>]+>[^<]+</script>", "")
//    println(str.replaceAll("<script>[^<]+</script>", "").replaceAll("<script[^>]+>[^<]+</script>", "").replaceAll("<[^>]+>", ""))
//    str = str.replaceAll("<script>[^<]+</script>", "")
//    println(str)
//    str = str.replaceAll("<script[^>]+>[^<]+</script>", "")
//    println(str)
//    str = str.replaceAll("<[^>]+>", "")
//    println(str)
    val content = common.html2str(str)
    val keyList = common.doc2keywords(content)
    val map = common.list2map(keyList)
    val set = map.keys
    println(content)
    println(keyList)
  }
}