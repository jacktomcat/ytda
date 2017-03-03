package com.gochinatv.ytda

import java.text.SimpleDateFormat
import java.util.Calendar

/**
  * Created by zhuhh on 16/12/21.
  */
object DateTools {

  var yyyyMMdd = new SimpleDateFormat("yyyy-MM-dd")

  /**
    * 获取当前格式话的时间
    * @return
    */
  def getCurrentTime(format:SimpleDateFormat):String = {
    val now = Calendar.getInstance().getTime()
    val times = format.format(now)
    times
  }


  def dateAdd(date:String,interval:Int):String={
    val calendar = Calendar.getInstance()
    val de = yyyyMMdd.parse(date)
    calendar.setTime(de)
    calendar.add(Calendar.DATE,interval)
    yyyyMMdd.format(calendar.getTime)
  }

}
