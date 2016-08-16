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

    //1. get hive and cassandra data
    TC_map.foreach { ele =>
      val TC_name = ele._1
      val TC_info = ele._2
      LogHolder.log.warn(s"--------------------------------- start to compare table: ${TC_name}")
      val hive_reg_table=s"hive${TC_name}"
      val cassa_reg_table=s"cassa${TC_name}"


      //1. get hive data
      var hiveDf_tmp = sqlContext.sql(TC_info.hive.sql).na.fill("").na.fill(0)
      TC_info.hive.drop.foreach(col => hiveDf_tmp = hiveDf_tmp.drop(col))

      val hiveDf = unifyFormat(hiveDf_tmp)
      hiveDf.registerTempTable(hive_reg_table)
      hiveDf.persist()

      val hiveCount = hiveDf.count()
      LogHolder.log.warn(s"1. Hive total records: ${hiveCount}")

      //2.get cassandra data
      var cassaDf_tmp = sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map("table"->TC_info.cassa.table,"keyspace"->TC_info.cassa.keyspace)).load().na.fill("").na.fill(0)
      TC_info.cassa.condition.foreach(filter=> cassaDf_tmp = cassaDf_tmp.where(filter))

      val rename_map = TC_info.cassa.rename
      rename_map.foreach(name => cassaDf_tmp = cassaDf_tmp.withColumnRenamed(name._1,name._2) )

      val cassanDf =unifyFormat(cassaDf_tmp)
      cassanDf.persist()
      val cassanCount = cassanDf.count()
      hiveDf.registerTempTable(cassa_reg_table)
      LogHolder.log.warn(s"2. cassandra total records: ${cassanCount}")


      //compare count
      if(hiveCount>cassanCount){
        LogHolder.log.warn (s"---- ERROR hive records are more than cassandra.  hive: ${hiveCount}, cassandra: ${cassanCount} !!!!!!!!!!!!!!!! ")
      }

      //-------------compare key
      val record_info = TC_info.record

      val primaryKeyCol = record_info.primaryKey.map(column).toSeq
      val hiveDfKey = hiveDf.select(primaryKeyCol:_*)
      val cassaDfKey = cassanDf.select(primaryKeyCol:_*)
      val excptDfKey = hiveDfKey.except(cassaDfKey)
      val diffkeynum = excptDfKey.count
      if(diffkeynum>0) {
        LogHolder.log.warn (s"---- ERROR lack ${record_info.careFiled} list")
        excptDfKey.select(record_info.careFiled).distinct.show(hiveCount.toInt)

        LogHolder.log.warn (s"---- ERROR Total ${diffkeynum} records are not in cassandra. details are below")
        excptDfKey.orderBy(record_info.careFiled).show(hiveCount.toInt)
      }

      //-------------------------compare record
      val comp_fields_ori = if((record_info.field)(0)=="*") hiveDf.columns else record_info.field
      val comp_fields= comp_fields_ori.filterNot(record_info.ignor.toSet).filterNot(record_info.primaryKey.toSet)

      val join_str = record_info.primaryKey.map(ele => s"${hive_reg_table}.${ele}=${cassa_reg_table}.${ele}").mkString(" AND ")
      val key_str = record_info.primaryKey.map(ele =>s"${hive_reg_table}.${ele}").mkString(",")
      val diff_str = comp_fields.map(ele => s" (CASE WHEN ${hive_reg_table}.${ele}=${cassa_reg_table}.${ele} THEN 0 ELSE 1 END) AS ${ele}").mkString(" , ")
      val filter_str = comp_fields.map(ele => s"B.${ele}==1").mkString(" OR ")
      val sql_str = s"SELECT B.* FROM (SELECT ${key_str}, ${diff_str} from ${hive_reg_table} join ${cassa_reg_table} on ${join_str})B WHERE ${filter_str}"

      val diffRec = sqlContext.sql(sql_str)

      val diffRecNum = diffRec.count
      if(diffRecNum>0) {
        LogHolder.log.warn(s"---- ERROR ${diffRec} records are not same, only ${hiveCount - diffRecNum} are same")
        diffRec.show
      }


//------------------------------------------------------
/*
      val comp_num = if(record_info.num>hiveCount) hiveCount.toInt else record_info.num
      //val comp_fields = if((record_info.field)(0)=="*") hiveDf.columns else record_info.field
      val comp_col_Seq= comp_fields.filterNot(record_info.ignor.toSet).map(column).toSeq

      val hiveDfComp = hiveDf.select(comp_col_Seq:_*)
      val cassaDfComp = cassanDf.select(comp_col_Seq:_*)
      val hiveTypeMap = hiveDfComp.dtypes.toMap
      val cassanTypeMap = cassaDfComp.dtypes.toMap

      if(hiveTypeMap!=cassanTypeMap){
        LogHolder.log.warn ("            ERROR FAILED  type is not same")
      }

      var tmp_compare_filed = Array()



      comp_fields.foreach{ tmp_key =>
        val tmp_compare_filed = Array.concat( primaryKey, Array(tmp_key))
        tmp_compare_filed.foreach(LogHolder.log.warn)
      }

      except_df.show(hiveCount.toInt)

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
        LogHolder.log.warn(filter_str)

        val canssan_num = cassanEleDf.count()
        if(canssan_num != 1){
          sucess_flag = false
          fail_num = fail_num + 1
          LogHolder.log.warn s"Condition: ${filter_str}"
          LogHolder.log.warn s"            ERROR found related ${canssan_num} records in cassandra"
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



      }*/

    }//TC_map

  }//main

  def unifyFormat(df:DataFrame):DataFrame={

    val dismissUnusedDt = udf((dt: String) => if(dt<"1950-01-01 00:00:00")"" else dt)

    var df_tmp = df
    val df_types = df.dtypes
    df_types.foreach { ele =>
      val field_name = ele._1
      val field_type = ele._2
      df_tmp = field_type match {
        case "StringType" => df_tmp
        case "IntegerType" => df_tmp
        case "BooleanType" => df_tmp
        case "TimestampType" =>
          val tmp = df_tmp.withColumn(field_name, date_format(df_tmp.col(field_name), "yyyy-MM-dd HH:mm:ss"))
          tmp.withColumn(field_name, dismissUnusedDt(tmp.col(field_name)))
        case _ => //decimal ,double....
          df_tmp.withColumn(field_name, df_tmp.col(field_name).cast(DecimalType(13, 2)))
      }
    }
    df_tmp
  }

}
