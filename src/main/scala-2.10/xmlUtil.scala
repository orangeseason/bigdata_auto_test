/**
  * Created by lisa on 8/12/16.
  */

import scala.xml.XML
import com.github.nscala_time.time.Imports._


case class hivInfo(sql:String,drop:Array[String])
case class cassaInfo(keyspace:String,table:String,condition:Array[String],rename:Map[String,String])
case class CountInfo(name:String)
case class RecordInfo(name:String, primaryKey:Array[String], careFiled:String, field:Array[String], ignor:Array[String] )
case class TcInfo(hive:hivInfo,cassa:cassaInfo,count:CountInfo,record:RecordInfo)

object xmlUtil {
  val test_xml_file = "/home/retail/houmei/auto_test/TC.xml"

  def TCparse(xmlfile:String = test_xml_file):Map[String,TcInfo]={

    val xml = XML.loadFile(xmlfile)
    val TClist = (xml \ "TESTCASE")
    TClist.map(TC=>((TC\"@name")(0).text, parseTCElement(TC))).toMap
  }

  def parseTCElement(TC:scala.xml.Node):TcInfo={

    val ystd_str = (DateTime.now-1.day).toString("yyyy-MM-dd")

    //hive
    val hive_Info = (TC \"HIVE")(0)
    val hive_sql = (hive_Info\"sql")(0).text.replace("fordtreplacement",ystd_str)
    val hive_drop = (hive_Info\"drop")(0).text.replace(" ","").split(",")
    val hive = new hivInfo(hive_sql,hive_drop)

    //Cassandra
    val cassan_info = (TC \"CASSANDRA")(0)
    val cassan_ks = (cassan_info\"keyspace")(0).text.replace(" ","")
    val cassan_table = (cassan_info\"table")(0).text.replace(" ","")
    val cassan_condition = (cassan_info\"condition")(0).text.toLowerCase.replace("fordtreplacement",ystd_str).split("and")

    var rename_map:Map[String,String]=Map()
    val hive_rename_list = (cassan_info\"rename")(0).text.replace(" ","").split(",")
    val len_renmae = hive_rename_list.length
    if(len_renmae> 0 && len_renmae%2==0){
      for (i <- 0 until len_renmae by 2) rename_map += (hive_rename_list(i)->hive_rename_list(i+1))
    }
    val cassa = new cassaInfo(cassan_ks,cassan_table,cassan_condition,rename_map)

    //compare
    val compare_list = (TC \"COMPARE").map(x=>((x\"@type")(0).text.replace(" ",""),x)).toMap
    val count = new CountInfo((compare_list("count")\"@name")(0).text)
    //record
    val compare_record = compare_list("record")
    val record_name = (compare_record\"@name")(0).text
    val record_primaryKey = (compare_record\"primaryKey")(0).text.replace(" ","").toLowerCase.split(",")
    val record_field = (compare_record\"field")(0).text.replace(" ","").toLowerCase.split(",")
    val record_ignor = (compare_record\"ignor")(0).text.replace(" ","").toLowerCase.split(",")
    val record_careFiled = (compare_record\"careFiled")(0).text.replace(" ","").toLowerCase

    val record = new RecordInfo(record_name,record_primaryKey,record_careFiled,record_field,record_ignor)

    new TcInfo(hive,cassa,count,record)
  }
}
