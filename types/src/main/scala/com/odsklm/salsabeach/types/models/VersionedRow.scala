package com.odsklm.salsabeach.types.models

import com.odsklm.salsabeach.types.ColumnDefs.FullColumn
import com.odsklm.salsabeach.types.rowconverters.RowKeyDecoder

import java.time.Instant

/**
  * A representation of an HBase Row, containing the rowKey and the row's column values.
  *
  * @param rowKey       the rowKey
  * @param columnsValueVersions the row's column values
  * @tparam A type of the rowKey
  */
case class VersionedRow[A: RowKeyDecoder](rowKey: A, columnsValueVersions: ColumnsValueVersions) {

  /**
    * Return the latest version of the value of a column at or before the timestamp
    *
    * @param column The column to get the value for
    * @param timestamp The timestamp at or before which to retrieve the value
    * @return The newest value at or before timestamp if the column exists, else None
    */
  def getValueAt(column: FullColumn, timestamp: Instant): Option[Value] =
    columnsValueVersions.get(column).flatMap(_.find(!_.timestamp.isAfter(timestamp)))

  /**
    * Return the latest version of the value of a column
    * @param column The column to get the value for
    * @return The newest value if the column exists and there is a value, else None
    */
  def getLatestValue(column: FullColumn): Option[Value] =
    columnsValueVersions.get(column).flatMap(_.headOption)

  /**
    * Return the latest snapshot of this Row with values for each column if a value exist for such a column
    * @return Row with latest values
    */
  def getLatestRow: Row[A] = {
    val columnsValues = columnsValueVersions
      .map {
        case (column, _) => (column, getLatestValue(column))
      }
      .collect {
        case (column, Some(value)) => (column, value)
      }
    Row(rowKey, columnsValues)
  }

  /**
    * Return a snaptshot of this VersionedRow at or beforea certain timestamp, with values for each column
    * if a value exists for that column
    * @param timestamp timestamp for which to get the values
    * @return Row with values at a certain moment in time
    */
  def getRowAt(timestamp: Instant): Row[A] = {
    val columnsValues = columnsValueVersions
      .map {
        case (column, _) => (column, getValueAt(column, timestamp))
      }
      .collect {
        case (column, Some(value)) => (column, value)
      }
    Row(rowKey, columnsValues)
  }
}
