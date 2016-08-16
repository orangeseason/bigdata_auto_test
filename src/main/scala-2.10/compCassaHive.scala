/**
  * Created by lisa on 8/12/16.
  */

import com.datastax.spark.connector.cql.CassandraConnector
import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DecimalType}
import org.apache.spark.{SparkConf, SparkContext}

object compCassaHive {

  val conf = new SparkConf()
  val sc = new SparkContext(conf)
  val sqlContext = new HiveContext(sc)
  import org.apache.spark.sql.functions._
  import sqlContext.implicits._

  def main(args: Array[String]) {

    val TC_map = xmlUtil.TCparse()

    var error_msgs = ""
    var sucess_flag = true

    //1. get hive and cassandra data
    TC_map.foreach { ele =>
      val TC_name = ele._1
      val TC_info = ele._2

      //1. get hive data
      var hiveDf_tmp = sqlContext.sql(TC_info.hive.sql).na.fill("").na.fill(0)
      TC_info.hive.drop.foreach(col =>
        hiveDf_tmp = hiveDf_tmp.drop(col)
      )
      val rename_map = TC_info.hive.rename
      rename_map.foreach(name =>
        hiveDf_tmp = hiveDf_tmp.withColumnRenamed(name._1,name._2)
      )

      val hiveDf = unifyFormat(hiveDf_tmp)
      hiveDf.persist()

      val hiveCount = hiveDf.count()
      println(s"Hive total records: ${hiveCount}")


      //2.get cassandra data
      var cassaDf_tmp = sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map("table"->TC_info.cassa.table,"keyspace"->TC_info.cassa.keyspace)).load().na.fill("").na.fill(0)
      TC_info.cassa.condition.foreach(filter=>
        cassaDf_tmp = cassaDf_tmp.where(filter)
      )

      val cassanDf =unifyFormat(cassaDf_tmp)
      cassanDf.persist()
      val cassanCount = cassanDf.count()
      println(s"cassandra total records: ${cassanCount}")


      //compare count
      if(hiveCount>cassanCount){
        println ("            ERROR FAILED  hive: %d, cassandra: %d !!!!!!!!!!!!!!!! ",hiveCount, cassanCount)
        error_msgs = error_msgs +  ("Records are missed in cassandra.  hive: %d, cassandra: %d  \n" ,hiveCount, cassanCount)
        sucess_flag=false
      }

      //-------------compare key
      val record_info = TC_info.record

      val primaryKeyCol = record_info.primaryKey.map(column).toSeq
      val hiveDfKey = hiveDf.select(primaryKeyCol:_*)
      val cassaDfKey = cassanDf.select(primaryKeyCol:_*)
      val excptDfKey = hiveDfKey.except(cassaDfKey)
      excptDfKey.show(hiveCount.toInt)



      //compare record

      val comp_num = if(record_info.num>hiveCount) hiveCount.toInt else record_info.num
      println(s"Began compare ${comp_num} records" )

      val comp_fields = if((record_info.field)(0)=="*") hiveDf.columns else record_info.field
      println (s"comp field ${comp_fields.mkString(",")}")

      val comp_col_Seq= comp_fields.filterNot(record_info.ignor.toSet).map(column).toSeq

      val hiveDfComp = hiveDf.select(comp_col_Seq:_*)
      val cassaDfComp = cassanDf.select(comp_col_Seq:_*)
      val hiveTypeMap = hiveDfComp.dtypes.toMap
      val cassanTypeMap = cassaDfComp.dtypes.toMap

      if(hiveTypeMap!=cassanTypeMap){
        println ("            ERROR FAILED  type is not same")
      }



      //------------------------
      val except_df = hiveDfComp.except(cassaDfComp)

      if(except_df.count > 0) {

        var tmp_compare_filed = Array()



        comp_fields.foreach{ tmp_key =>
          val tmp_compare_filed = Array.concat( primaryKey, Array(tmp_key))
          tmp_compare_filed.foreach(println)
        }

        except_df.show(hiveCount.toInt)
      }


      var fail_num = 0

      hiveDfComp.take(comp_num).foreach{hive_row =>

        var filter_str=""

        var cassanEleDf = cassanDf

        record_info.primaryKey.foreach{ filter_key =>

          val filter_value = hiveTypeMap(filter_key) match {
            case "StringType" => hive_row.getAs[String](filter_key)
            case "TimestampType" => hive_row.getAs[DateTime](filter_key)
            case "IntegerType" => hive_row.getAs[Int](filter_key)
            case _ =>  //boolean, decimal ,double....
              hive_row.getAs[String](filter_key)
          }

          val filter_condition_str = cassanTypeMap(filter_key) match {
            case "StringType" => s"${filter_key} = '${filter_value}'"
            case "TimestampType" => s"${filter_key} = '${filter_value}'"
            case _ =>  //boolean, decimal ,double....
              s"${filter_key} = ${filter_value}"
          }

          if(filter_str=="") filter_str = filter_condition_str else filter_str = filter_str + " AND " + filter_condition_str

          cassanEleDf = cassanEleDf.where(filter_condition_str)
        }
        println(filter_str)

        val canssan_num = cassanEleDf.count()
        if(canssan_num != 1){
          sucess_flag = false
          fail_num = fail_num + 1
          println s"Condition: ${filter_str}"
          println s"            ERROR found related ${canssan_num} records in cassandra"
        }
        else{
          val cassan_row = (cassanEleDf.take(1))(0)
          val hive_value = hiveTypeMap(filter_key) match {
            case "StringType" => hive_row.getAs[String](filter_key)
            case "TimestampType" => hive_row.getAs[DateTime](filter_key)
            case "IntegerType" => hive_row.getAs[Int](filter_key)
            case _ =>  //boolean, decimal ,double....
              hive_row.getAs[String](filter_key)
          }


        }



      }


    }







  }

  def unifyFormat(df:DataFrame):DataFrame={

    var df_tmp = df
    val df_types = df.dtypes
    df_types.foreach { ele =>
      val field_name = ele._1
      val field_type = ele._1
      df_tmp = field_type match {
        case "StringType" => df_tmp
        case "TimestampType" => df_tmp.withColumn(field_name, date_format(df_tmp.col(field_name), "yyyy-MM-dd HH:mm:ss"))
        case "BooleanType" => df_tmp
        case _ => //boolean, decimal ,double....
          df_tmp.withColumn(field_name, df_tmp.col(field_name).cast(DecimalType(13, 0)))
      }
    }
    df_tmp
  }

}
