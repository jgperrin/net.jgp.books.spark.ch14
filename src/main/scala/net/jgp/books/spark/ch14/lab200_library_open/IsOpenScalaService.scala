package net.jgp.books.spark.ch14.lab200_library_open

import java.sql.Timestamp
import java.util.Calendar

trait IsOpenScalaService {

  def isOpen(hoursMon: String, hoursTue: String, hoursWed: String,
             hoursThu: String, hoursFri: String, hoursSat: String,
             hoursSun: String, dateTime: Timestamp): Boolean
}

object IsOpenScalaService extends IsOpenScalaService {
  def isOpen(hoursMon: String, hoursTue: String, hoursWed: String,
             hoursThu: String, hoursFri: String, hoursSat: String,
             hoursSun: String, dateTime: Timestamp): Boolean = {
    // get the day of the week
    val cal = Calendar.getInstance
    cal.setTimeInMillis(dateTime.getTime)
    val day = cal.get(Calendar.DAY_OF_WEEK)
    println(s"Day of the week: ${day}")
    val hours = day match {
      case Calendar.MONDAY =>
        hoursMon

      case Calendar.TUESDAY =>
        hoursTue

      case Calendar.WEDNESDAY =>
        hoursWed

      case Calendar.THURSDAY =>
        hoursThu

      case Calendar.FRIDAY =>
        hoursFri

      case Calendar.SATURDAY =>
        hoursSat

      case _ => // Sunday
        hoursSun
    }

    // quick return
    if (hours.compareToIgnoreCase("closed") == 0)
      return false
    // check if in interval
    val event = cal.get(Calendar.HOUR_OF_DAY) * 3600 + cal.get(Calendar.MINUTE) * 60 + cal.get(Calendar.SECOND)
    val ranges = hours.split(" and ")
    for (i <- 0 until ranges.length) {
      println(s"Processing range #${i}: ${ranges(i)}")
      val operningHours = ranges(i).split("-")

      val start = Integer.valueOf(operningHours(0).substring(0, 2)) * 3600
      +Integer.valueOf(operningHours(0).substring(3, 5)) * 60

      val end = Integer.valueOf(operningHours(1).substring(0, 2)) * 3600
      +Integer.valueOf(operningHours(1).substring(3, 5)) * 60

      println(s"Checking between ${start} and ${end}")
      if (event >= start && event <= end)
        return true
    }
    false

  }
}
