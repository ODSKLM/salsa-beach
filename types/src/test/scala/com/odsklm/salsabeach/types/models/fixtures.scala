package com.odsklm.salsabeach.types.models

import java.time.{ZoneOffset, ZonedDateTime}
import java.time.temporal.ChronoUnit

object fixtures {
  val morning = ZonedDateTime.of(2020, 1, 1, 9, 0, 0, 0, ZoneOffset.UTC).toInstant
  val afternoon1PM = morning.plus(4, ChronoUnit.HOURS)
  val afternoon2PM = morning.plus(5, ChronoUnit.HOURS)
  val evening9PM = morning.plus(12, ChronoUnit.HOURS)
  val value1 = Value("value1", morning)
  val value2 = Value("value2", afternoon1PM)
  val value3 = Value("value3", afternoon1PM)
  val value4 = Value("value4", evening9PM)
}
