package com.odsklm

import java.time.Instant

import com.odsklm.salsabeach.types.ColumnDefs._
import com.odsklm.salsabeach.types.models.{ColumnsValueVersions, ColumnsValues, Value}
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.{Filter, FilterList, PrefixFilter, SingleColumnValueFilter}
import org.apache.hadoop.hbase.io.TimeRange
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, CellUtil}

import scala.jdk.CollectionConverters._
import scala.collection.SortedSet

package object salsabeach {

  /**
    * To make conversions from Bytes to String
    */
  implicit private[salsabeach] def stringToBytes(
      str: String
    ): Array[Byte] = Bytes.toBytes(str)

  /* Some filter checks here.
   * If a filter is a FilterList, do a recursive call for all filters contained in the filter list.
   * If a filter is a PrefixFilter, we need to set the start row (and end row), otherwise the entire table will be scanned. This is done by setRowPrefixFilter.
   * If a filter is a SingleColumnValueFilter, we need to add the column specifically to the scanner, otherwise the filter has no effect.
   * Caution: this will mutate the supplied Scan object!
   */
  private[salsabeach] def checkFilter(scan: Scan, f: Filter): Unit =
    f match {
      case fList: FilterList =>
        for (fltr <- fList.getFilters.asScala)
          checkFilter(scan, fltr)
      case pf: PrefixFilter =>
        scan.setRowPrefixFilter(pf.getPrefix)
      case scvf: SingleColumnValueFilter =>
        scan.addColumn(scvf.getFamily, scvf.getQualifier)
      case _ =>
    }

  /**
    * Converts raw cell data from HBase to a Map of columns to sets of values
    *
    * @param result  A HBase result object
    * @param columns A list of columns part of the result
    * @return A Mapping from FullColumn to Values.
    */
  private[salsabeach] def convertCellsToColumnsWithValues(
      result: Result,
      columns: Seq[FullColumn]
    ): ColumnsValues = {
    val columnsOptionalValues = columns.map(col =>
      col -> cell2OptionalValue(result.getColumnLatestCell(col.columnFamily, col.name))
    )
    columnsOptionalValues.collect {
      case (column, Some(value)) => (column, value)
    }.toMap
  }

  /**
    * Converts raw cell data from HBase to a Map of columns to sets of versioned values
    *
    * @param result  A HBase result object
    * @param columns A list of columns part of the result
    * @return A Mapping from FullColumn to sets of versioned Values.
    */
  private[salsabeach] def convertCellsToColumnsWithVersionedValues(
      result: Result,
      columns: Seq[FullColumn]
    ): ColumnsValueVersions = {
    val hBaseCells = result.rawCells.toList

    val columnNamesToColumn =
      columns.map(column => column.name -> Some(column)).toMap.withDefaultValue(None)

    val columnsWithCells = hBaseCells
      .flatMap { cell =>
        val columnName = getColumnName(cell)
        val columnValue = cell2Value(cell)
        val column = columnNamesToColumn(columnName)
        column.map(column => column -> columnValue)
      }
      .groupBy(x => x._1)

    columnsWithCells.map {
      case (column, cells) =>
        column -> SortedSet(cells.map(_._2): _*)
    }
  }

  /**
    * Converts raw cell data from HBase of a specific column family to a Map of dynamically created column objects
    * to values
    *
    * @param result         A HBase result object
    * @param columnFamily   column family for which the result was retrieved
    * @param columnFactory  a partial function that creates FullColumns based on a column name as input
    * @tparam CF            ColumnFamily trait of which `columnFamily` is an instance
    * @tparam C             Column type that is both a FullColumn and has CF as column family
    * @return A mapping from dynamic column objects to their corresponding values
    */
  private[salsabeach] def convertCellsToDynamicColumsWithValues[
      CF <: ColumnFamily,
      C <: Column with CF
    ](result: Result,
      columnFamily: CF,
      columnFactory: PartialFunction[String, C]
    ): ColumnsValues = {
    val hBaseCells = result.rawCells.toList
    hBaseCells
      .map { cell =>
        val columnName = getColumnName(cell)
        val columnValue = cell2Value(cell)
        val cf = getColumnFamily(cell)
        if (cf == columnFamily.columnFamily && columnFactory.isDefinedAt(columnName))
          Some(columnFactory(columnName) -> columnValue)
        else
          None
      }
      .collect {
        case Some((column, value)) => (column, value)
      }
      .toMap
  }

  private[salsabeach] def convertCellsToVersionedValues(
      result: Result,
      column: FullColumn
    ): SortedSet[Value] = {
    val hBaseCells = result.getColumnCells(column.columnFamily, column.name).asScala.toList
    val values = hBaseCells.map(cell2Value)
    SortedSet(values: _*)
  }

  /**
    * Get the name of the column of a cell
    *
    * @param c The cell
    * @return The name
    */
  private[salsabeach] def getColumnName(c: Cell): String =
    convertToString(c.getQualifierArray, c.getQualifierOffset, c.getQualifierLength)

  /**
    * Get the column family of a cell
    * @param c the cell
    * @return the column family
    */
  private[salsabeach] def getColumnFamily(c: Cell): String =
    convertToString(c.getFamilyArray, c.getFamilyOffset, c.getFamilyLength)

  /**
    * Converts bytes to String
    *
    * @param bytes  The bytes array
    * @param offset The offset
    * @param length The length
    * @return The String representation
    */
  private[salsabeach] def convertToString(bytes: Array[Byte], offset: Int, length: Int) =
    Bytes.toString(Bytes.copy(bytes, offset, length))

  /**
    * Convert an HBase Cell to a Salsa Beach Value
    *
    * @param cell Cell from HBase Result
    * @return Value for one rowkey, column, timestamp combination
    */
  private[salsabeach] def cell2Value(cell: Cell): Value =
    Value(Bytes.toString(CellUtil.cloneValue(cell)), Instant.ofEpochMilli(cell.getTimestamp))

  /**
    * Convert an HBase Cell that is possibly `null` to an Optional Salsa Beach Value
    *
    * @param cell Cell from HBase Result, can be `null`
    * @return Optional Salsa Beach Value
    */
  private[salsabeach] def cell2OptionalValue(cell: Cell): Option[Value] =
    Option(cell).map(cell2Value)

  private[salsabeach] def calculateTimeRange(
      minTimestamp: Option[Instant] = None,
      maxTimestamp: Option[Instant] = None
    ): Option[(Long, Long)] = (minTimestamp, maxTimestamp) match {
    case (Some(minTS), None) =>
      Some((minTS.toEpochMilli, TimeRange.INITIAL_MAX_TIMESTAMP))
    case (None, Some(maxTS)) =>
      Some((TimeRange.INITIAL_MIN_TIMESTAMP, maxTS.toEpochMilli))
    case (Some(minTS), Some(maxTS)) => Some((minTS.toEpochMilli, maxTS.toEpochMilli))
    case _                          => None
  }
}
