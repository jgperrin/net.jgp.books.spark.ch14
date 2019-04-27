package net.jgp.books.sparkInAction.ch14.lab100_library_open;

import java.sql.Timestamp;
import java.util.Calendar;

import org.apache.spark.sql.api.java.UDF8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IsOpenUdf implements
    UDF8<String, String, String, String, String, String, String, Timestamp,
        Boolean> {
  private static Logger log = LoggerFactory.getLogger(IsOpenUdf.class);
  private static final long serialVersionUID = -216751L;

  public IsOpenUdf() {
  }

  // @Override
  // public Boolean call(String range, Integer event) throws Exception {
  // log.debug("-> call({}, {})", range, event);
  // String[] ranges = range.split(";");
  // for (int i = 0; i < ranges.length; i++) {
  // log.debug("Processing range #{}: {}", i, ranges[i]);
  // String[] hours = ranges[i].split("-");
  // int start =
  // Integer.valueOf(hours[0].substring(0, 2)) * 3600 +
  // Integer.valueOf(hours[0].substring(3)) * 60;
  // int end =
  // Integer.valueOf(hours[1].substring(0, 2)) * 3600 +
  // Integer.valueOf(hours[1].substring(3)) * 60;
  // log.debug("Checking between {} and {}", start, end);
  // if (event >= start && event <= end) {
  // return true;
  // }
  // }
  // return false;
  // }

  @Override
  public Boolean call(
      String hoursMon, String hoursTue,
      String hoursWed, String hoursThu,
      String hoursFri, String hoursSat,
      String hoursSun,
      Timestamp dateTime) throws Exception {

    // get the day of the week
    Calendar cal = Calendar.getInstance();
    cal.setTimeInMillis(dateTime.getTime());
    int day = cal.get(Calendar.DAY_OF_WEEK);
    log.debug("Day of the week: {}", day);

    String hours;
    switch (day) {
      case Calendar.MONDAY:
        hours = hoursMon;
        break;

      case Calendar.TUESDAY:
        hours = hoursTue;
        break;

      case Calendar.WEDNESDAY:
        hours = hoursWed;
        break;

      case Calendar.THURSDAY:
        hours = hoursThu;
        break;

      case Calendar.FRIDAY:
        hours = hoursFri;
        break;
      case Calendar.SATURDAY:
        hours = hoursSat;
        break;

      default: // Sunday
        hours = hoursSun;
    }

    if(hours.compareToIgnoreCase("closed") == 0) {
      return false;
    }
    
    int event = cal.get(Calendar.HOUR_OF_DAY) * 3600
        + cal.get(Calendar.MINUTE) * 60
        + cal.get(Calendar.SECOND);

    String[] ranges = hours.split(" and ");
    for (int i = 0; i < ranges.length; i++) {
      log.debug("Processing range #{}: {}", i, ranges[i]);
      String[] operningHours = ranges[i].split("-");
      int start =
          Integer.valueOf(operningHours[0].substring(0, 2)) * 3600 +
              Integer.valueOf(operningHours[0].substring(3, 5)) * 60;
      int end =
          Integer.valueOf(operningHours[1].substring(0, 2)) * 3600 +
              Integer.valueOf(operningHours[1].substring(3, 5 )) * 60;
      log.debug("Checking between {} and {}", start, end);
      if (event >= start && event <= end) {
        return true;
      }
    }
    
    return false;
  }

}
