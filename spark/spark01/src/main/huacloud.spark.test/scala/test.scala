import com.typesafe.config.ConfigFactory

/**
 * Created by Administrator on 2018/12/31.
 */
object test {
  def main(args: Array[String]): Unit = {
    val str= "Bhyxtqwz001@jxsg"
    var resultStr :String =null
    if(str!=null){
      resultStr=""
      if(str.contains("@")){
        val index = str.indexOf("@")
        for(chr <-str.substring(0,index).toCharArray){
          resultStr+="*"

        }
        resultStr+=str.substring(index)
      }else{
        resultStr =str
      }
    }
    print(resultStr)
  }

}
