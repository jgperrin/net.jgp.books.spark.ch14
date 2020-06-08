package net.jgp.books.spark.ch14.lab900_in_range


trait InRangeScalaService {

  @throws[Exception]
  def call(range: String, event: Integer): Boolean

}

object InRangeScalaService extends InRangeScalaService {

  @throws[Exception]
  override def call(range: String, event: Integer): Boolean = {
    println(s"-> call(${range}, ${event})")
    val ranges = range.split(";")
    for (i <- 0 until ranges.length) {
      println(s"Processing range #${i}: ${ranges(i)}")
      val hours = ranges(i).split("-")

      val start = Integer.valueOf(hours(0).substring(0, 2)) * 3600 +
        Integer.valueOf(hours(0).substring(3)) * 60

      val end = Integer.valueOf(hours(1).substring(0, 2)) * 3600 +
        Integer.valueOf(hours(1).substring(3)) * 60

      println(s"Checking between ${start} and ${end}")
      if (event >= start && event <= end) return true
    }
    false
  }

}