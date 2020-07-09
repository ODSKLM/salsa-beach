package com.odsklm.salsabeach.types

import java.time.Instant

import com.odsklm.salsabeach.types.ColumnDefs.FullColumn

import scala.collection.SortedSet

package object models {

  /**
    * A representation of a versioned value from HBase
    *
    * @param value     The value
    * @param timestamp The timestamp when this value was written
    */
  case class Value(value: String, timestamp: Instant)

  /**
    * Ordering for Value. Orders first on timestamp descending, then on value
    */
  implicit val valueOrdering: Ordering[Value] =
    Ordering.by(value => (-value.timestamp.toEpochMilli, value.value))

  type ColumnsValues = Map[FullColumn, Value]

  type ColumnsValueVersions = Map[FullColumn, SortedSet[Value]]
}
