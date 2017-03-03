package com.gochinatv.ytda

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by zhuhh on 16/12/19.
  * 获取yt源数据
  */
object YoutubeSrcData {


  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("YoutubePlayCountTask")//.setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    //val sqlContext = new SQLContext(sc)
    val sqlContext = new HiveContext(sc)

    val dataFrame = sqlContext.read.format("jdbc").options(Map(
      "driver"-> "org.postgresql.Driver",
      "url" -> "jdbc:postgresql://10.2.0.234:5432/vrs_dev",
      "dbtable" -> "episodes",
      "user" -> "postgres",
      "password" -> "postgres"
    )).load()

    sqlContext.sql("use vrs")

    //把前一天的数据拷贝到 episodes_yesterday_count
    sqlContext.sql("INSERT OVERWRITE TABLE episodes_yesterday_count select * from episodes_now_count")

    //这里直接把最新的播放情况从pg数据直接保存到vrs的episodes_now_count表中
    dataFrame.select("id","yt_view_count").write.mode(SaveMode.Overwrite).saveAsTable("episodes_now_count")


    //对比episodes_now_count,episodes_yesterday_count 数据相减得出每天的增量,把增量数据追加到 episodes_day_count
    val time = DateTools.getCurrentTime(DateTools.yyyyMMdd)

    //不支持这种写法换成下面的写法
    /*val inc_sql="INSERT TABLE episodes_day_count select xxxx "
    sqlContext.sql(inc_sql)*/

    /*val data = sqlContext.sql(
      "select " +
      "enc.id id," +
      "enc.yt_view_count now_view_count," +
      "eyc.yt_view_count yesterday_view_count," +
      "if(enc.yt_view_count is NULL,0,enc.yt_view_count)-if(eyc.yt_view_count is NULL,0,eyc.yt_view_count) today_view_count, " +
      "'"+time+"' view_date "+
      "from episodes_now_count enc left join episodes_yesterday_count eyc on enc.id=eyc.id")
    data.write.mode("append").saveAsTable("episodes_day_count")*/

    sqlContext.sql(
        "INSERT OVERWRITE TABLE episodes_day_count PARTITION(view_date='"+time+"') "+
        "select " +
        "enc.id id," +
        "enc.yt_view_count now_view_count," +
        "eyc.yt_view_count yesterday_view_count," +
        "if(enc.yt_view_count is NULL,0,enc.yt_view_count)-if(eyc.yt_view_count is NULL,0,eyc.yt_view_count) today_view_count, " +
        "'"+time+"' view_date "+
        "from episodes_now_count enc left join episodes_yesterday_count eyc on enc.id=eyc.id")


    //TODO 可以提取到第二个任务,因为这个时间以后可以是任何的时间的增量数据
    val week_ago = DateTools.dateAdd(time,-7);
    //计算并update pg数据库
    val pgFrame = sqlContext.sql("" +
                   "select id,sum(now_view_count) weekly_play_count from episodes_day_count " +
                   "where view_date between '"+week_ago+"' and '"+time+"' group by id")

    val prop = new java.util.Properties
    prop.setProperty("driver","org.postgresql.Driver")
    prop.setProperty("user","postgres")
    prop.setProperty("password","postgres")
    pgFrame.write.mode(SaveMode.Overwrite).jdbc("jdbc:postgresql://10.2.0.234:5432/vrs_dev","episodes_weekly",prop)

  }

  /*def updatePostgresql():Unit = {
      var conn: Connection = null
      var ps: PreparedStatement = null
      val sql = "update episodes set (weekly_play_count)=(" +
                "select weekly_play_count from episodes_weekly where episodes_weekly.id=episodes.id) " +
                "where episodes.id=196496"
      try {
        conn = DriverManager.getConnection("jdbc:postgresql://10.2.0.234:5432/vrs_dev","postgres", "postgres")
        ps = conn.prepareStatement(sql)
        ps.executeUpdate()
      } catch {
        case e: Exception => println("Mysql Exception")
      } finally {
        if (ps != null) {
          ps.close()
        }
        if (conn != null) {
          conn.close()
        }
      }
  }*/

}
