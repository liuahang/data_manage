import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql._


/**
 * Created by Wping on 2018/12/28.
 */
object oracle2File_etl {

  val config = ConfigFactory.load()
  val outputPathPrefix="file:///root/export_tmp_files/"
  def main(args: Array[String]): Unit = {

    val DB = "jxszfw"
    val insensitiveTableList = List("JX_ZZ_ISSUEHASTYPES",
                                    "JX_ZZ_ISSUESTEPGROUPS",
                                    "JX_ZZ_ISSUETYPEDOMAINS",
                                    "ZJS_ZZ_PROPERTYDOMAINS",
                                    "ZJS_ZZ_ISSUETYPES",
                                    "ZJS_ZZ_ORGANIZATIONS",
                                    "ZJS_ZZ_PROPERTYDICTS"
    )
    val sensitiveTableAndFilterSqlMap = Map(
      "JX_ZZ_ISSUELOGS"-> """
                            |SELECT
                            | id
                            |,issueid
                            |,dealorgid
                            |,targeorgid
                            |,forissuelogid
                            |,backissuelogid
                            |,dealstepindex
                            |,returntoissuelogid
                            |,supervisionstate
                            |,dealtype,dealstate
                            |,name_rm_sensitive(dealusername) AS dealusername
                            |,phone_rm_sensitive(mobile) AS mobile
                            |,dealdescription,targeorginternalcode,stateclass
                            |,name_rm_sensitive(createuser) AS createuser
                            |,name_rm_sensitive(updateuser) AS updateuser
                            |,dealtime
                            |,dealdeadline
                            |,logcompletetime
                            |,forlogentrytime
                            |,createdate
                            |,updatedate
                            |,content
                            |,stepid
                            |,pid
                            |,tq_isdelete
                            |,tq_lastmodified
                            |,dealorginternalcode
                            |,ldsjc
                            |FROM sourceData
                          """.stripMargin
      ,
      "JX_ZZ_ISSUERELATEDPEOPLE"-> """
                                    |SELECT
                                    | id
                                    |,name_rm_sensitive(name) AS name
                                    |,phone_rm_sensitive(telephone) AS telephone
                                    |,issueid
                                    |,peopleid
                                    |,peopleactualtype
                                    |,tq_isdelete
                                    |,tq_lastmodified
                                    |,ldsjc
                                    |FROM sourceData
                                  """.stripMargin
      ,
      "JX_ZZ_ISSUES"-> """
                        |SELECT
                        | tq_isdelete
                        |,tq_lastmodified
                        |,currentorg
                        |,currentorgdisplayname
                        |,name_rm_sensitive(sourceperson) AS sourceperson
                        |,phone_rm_sensitive(sourcemobile) AS sourcemobile
                        |,occurlocation
                        |,name_rm_sensitive(maincharacters) AS maincharacters
                        |,historic
                        |,completedate
                        |,status
                        |,issuetype
                        |,name_rm_sensitive(undertakeusername) AS undertakeusername
                        |,name_rm_sensitive(undertakemobile) AS undertakemobile
                        |,buildingid
                        |,centerx
                        |,centery
                        |,mail_rm_sensitive(createuser) AS createuser
                        |,mail_rm_sensitive(updateuser) AS updateuser
                        |,occurdate
                        |,createdate
                        |,updatedate
                        |,lat
                        |,lon
                        |,centerlon
                        |,centerlat
                        |,zoom
                        |,name_rm_sensitive(createperson) AS createperson
                        |,createpersonid
                        |,sentimenet
                        |,dealtime
                        |,comments
                        |,degree
                        |,hours
                        |,minute
                        |,fourcolorwarningissueflag
                        |,centerlon2
                        |,centerlat2
                        |,dockingstatus
                        |,targetorg
                        |,lastdealdate
                        |,superviselevel
                        |,statecode
                        |,name_rm_sensitive(ownerperson) AS ownerperson
                        |,selfdomissuetypeorgcode
                        |,isintofire
                        |,isintoletter
                        |,completelevel
                        |,issupervisiontag
                        |,isadvicechecking
                        |,id
                        |,occurorg
                        |,createorg
                        |,lastorg
                        |,issuekind
                        |,sourcekind
                        |,relatepeoplecount
                        |,displaystyle
                        |,currentstep
                        |,relateamount
                        |,important
                        |,urgent
                        |,isemergency
                        |,serialnumber
                        |,subject
                        |,occurorginternalcode
                        |,createorginternalcode
                        |,lastorginternalcode
                        |,name_rm_sensitive(lastusername) AS lastusername
                        |,issuecontent
                        |,remark
                        |,ldsjc
                        |FROM sourceData
                       """.stripMargin
      ,
      "JX_ZZ_ISSUESTEPS"-> """
                            |SELECT
                            | id
                            |,source
                            |,sourceinternalcode
                            |,target
                            |,targetinternalcode
                            |,entryoperate
                            |,entrydate
                            |,enddate
                            |,lastdealdate
                            |,superviselevel
                            |,backto
                            |,state
                            |,createuser
                            |,updateuser
                            |,createdate
                            |,updatedate
                            |,forissuelogid
                            |,dealstepindex
                            |,returntoissuelogid
                            |,dealtype
                            |,dealstate
                            |,name_rm_sensitive(dealusername) AS dealusername
                            |,phone_rm_sensitive(mobile) AS mobile
                            |,dealdescription
                            |,logcompletetime
                            |,forlogentrytime
                            |,content
                            |,pid
                            |,tq_isdelete
                            |,tq_lastmodified
                            |,statecode
                            |,issue
                            |,groupid
                            |,ldsjc
                            |FROM sourceData
                          """.stripMargin
    )
    val spark = SparkSession.builder
	    .config(new SparkConf())
      .master("local[4]")
      .appName("oracle2File_jxszfw")
      .getOrCreate()
    //不需要特殊操作的表直接写入文件
    for(table<-insensitiveTableList){
      val filtedDF = createDataFrame(spark,DB,table)
      val path = outputPathPrefix+DB+"/"+table
      writeIntoFile(filtedDF,path)
    }
    //处理需要进一步操作（如脱敏）的表
    for((table,sql)<-sensitiveTableAndFilterSqlMap){
      val filtedDF = createDataFrame(spark,DB,table,true,sql)
      val path = outputPathPrefix+DB+"/"+table
      writeIntoFile(filtedDF,path)
    }
  }


   def writeIntoFile(df:DataFrame,path:String){
     df.write
       .format("csv")
       .option("header", "true")
       .option("delimiter"," ")
       .mode("overwrite")
       .save(path)
   }
  def createDataFrame(spark:SparkSession,DBName:String,tableName:String):DataFrame={
    return  createDataFrame(spark,DBName,tableName,false,null)
  }
  def createDataFrame(spark:SparkSession,DBName:String,tableName:String,needRmSensitive: Boolean,filterSql: String):DataFrame={
    val dbtable= DBName+"."+tableName
    val df =spark.read
      .format("jdbc")
      .option("url",config.getString(Constants.ORACLE210_DB_URL))
      .option("user", config.getString(Constants.ORACLE_DB_USRNAME))
      .option("password",config.getString(Constants.ORACLE_DB_PASSWORD))
      .option("driver",config.getString(Constants.ORACLE_DB_DRIVER))
      .option("dbtable",dbtable)
      .option("query","select * from "+tableName+" WHERE ldsjc >= '2018-09-01' ORDER BY ldsjc")
      .load()
    needRmSensitive match {
      case true =>{
        df.createOrReplaceTempView("sourceData")
        registerUdfs(spark)
        spark.sql(filterSql)
      }
      case _ => df
    }
  }

  def registerUdfs(spark:SparkSession) ={

    spark.udf.register("name_rm_sensitive",(str:String) =>{
      var resultStr :String =""
      if(str!=null && str.length>1){
        resultStr +=str.charAt(0)
        for(chr <-str.substring(1).toCharArray){
          resultStr+="*"
        }
      }else{
        resultStr = str
      }
      resultStr
    })
    spark.udf.register("phone_rm_sensitive",(str:String)=>{
      var resultStr :String =null
      val telephone_pattern = "0?(13|14|15|18|17)[0-9]{9}"
      if(str!=null){
        if(str.matches(telephone_pattern)){
          resultStr = str.substring(0,3)+"****"+str.substring(7)
        }else if(str.length > 3){
          resultStr = str.substring(0,3)
          for(chr <-str.substring(3).toCharArray ){
            if(chr.toString.matches("[0-9]")){
              resultStr+="*"
            }else{
              resultStr+=chr
            }
          }
        }else{
          resultStr = str
        }
      }
      resultStr
    })
    spark.udf.register("cerdID_rm_sensitive",(str:String)=>{
      var resultStr :String =null
      val cerdID_pattern = """\d{17}[\d|x]|\d{15}"""
      if(str!=null){
        if(str.matches(cerdID_pattern)){
          if(str.length==15){
            resultStr=str.substring(0,6)+"******"+str.substring(12)
          }else if(str.length==18){
            resultStr=str.substring(0,6)+"********"+str.substring(14)
          }
        }else{
          resultStr = str
        }
      }
      resultStr
    })

    spark.udf.register("mail_rm_sensitive",(str:String)=>{
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
      resultStr
    })

  }
}
