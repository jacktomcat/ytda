package com.gochinatv.ytda

import java.text.SimpleDateFormat
import java.util.Calendar

/**
  * Created by zhuhh on 16/12/21.
  */
object TestYoutube {


  def main(args: Array[String]) {
    val now = Calendar.getInstance().getTime()
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val times = format.format(now)

    val inc_sql="INSERT TABLE episodes_day_count " +
      "select " +
      "enc.id," +
      "enc.yt_view_count now_view_count," +
      "eyc.yt_view_count yesterday_view_count," +
      "enc.yt_view_count - eyc.yt_view_count today_view_count, " +
      "'"+times+"' view_date "+
      "from episodes_now_count enc left join episodes_yesterday_count eyc on enc.id=eyc.id"

    println(inc_sql)
  }
}
