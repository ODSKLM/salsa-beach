package com.odsklm.salsabeach

import java.time.Instant

import com.odsklm.salsabeach.types.ColumnDefs._
import com.odsklm.salsabeach.types.models.Row
import com.odsklm.salsabeach.types.rowconverters
import com.odsklm.salsabeach.types.rowconverters.{RowKeyDecoder, RowQueryEncoder}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.hbase.client.{ResultScanner, Scan, Table}
import org.apache.hadoop.hbase.filter.Filter
import com.odsklm.salsabeach.types.rowconverters.RowQueryEncoder.ops._
import org.apache.hadoop.hbase.util.Bytes

import scala.jdk.CollectionConverters._

// TODO: Make scan operations return a Mapping instead of List of Rows?
object ScanOps extends LazyLogging {

  /**
    * Get multiple columns from table from rows with a given row prefix
    *
    * @param rowKeyPrefix The prefix (left string) of the rows to scan
    * @param columns      A list of columns (consisting of column family and column)
    * @param table        (implicit) The table to read from
    * @tparam P           Type of row key prefix for which `RowQueryEncoder` needs to exist
    * @tparam K           Type of row key for which `RowKeyDecoder` needs to exist. Used to decode row keys of result
    * @return List of Rows with each a row key and a mapping from column to value
    */
  def scan[P: RowQueryEncoder, K: RowKeyDecoder](
      rowKeyPrefix: P,
      columns: List[FullColumn]
    )(implicit table: Table
    ): List[Row[K]] = {
    val scan = createRowPrefixScan(rowKeyPrefix = Some(rowKeyPrefix), columns = columns)
    execute(scan, columns)
  }

  /**
    *
    * @param rowKeyPrefix The prefix (left string) of the rows to scan
    * @param columns      A list of columns (consisting of column family and column)
    * @param timestamp    The moment for which the result is to be retrieved. HBase update timestamps of the resulting
    *                     values will be at or before this timestamp.
    *                     NOTE: this behaviour has changed compared to previous versions of Salsa Beach, which used
    *                     `timestamp` exclusive instead of inclusive.
    *                     The new behaviour is more in line with that of the HBase Java client.
    * @param table        (implicit) The table to read from
    * @tparam P           Type of row key prefix for which `RowQueryEncoder` needs to exist
    * @tparam K           Type of row key for which `RowKeyDecoder` needs to exist. Used to decode row keys of result
    * @return List of Rows with each a row key and a mapping from column to value at the specified moment in time
    */
  def scan[P: RowQueryEncoder, K: RowKeyDecoder](
      rowKeyPrefix: P,
      columns: List[FullColumn],
      timestamp: Instant
    )(implicit table: Table
    ): List[Row[K]] = {
    val ts = Instant.ofEpochMilli(timestamp.toEpochMilli + 1)
    val scan =
      createRowPrefixScan(
        rowKeyPrefix = Some(rowKeyPrefix),
        columns = columns,
        maxTimestamp = Some(ts)
      )
    execute(scan, columns)
  }

  /**
    * Get all rows for the given column from table with a given row prefix (filter).
    *
    * @param rowKeyPrefix The prefix (left string) of the rows to scan
    * @param column       Column with family
    * @param table        (implicit) The table to read from
    * @tparam P           Type of row key prefix for which `RowQueryEncoder` needs to exist
    * @tparam K           Type of row key for which `RowKeyDecoder` needs to exist
    * @return List of Rows with each a row key and a mapping from column to value
    */
  def scan[P: RowQueryEncoder, K: RowKeyDecoder](
      rowKeyPrefix: P,
      column: FullColumn
    )(implicit table: Table
    ): List[Row[K]] =
    scan(rowKeyPrefix, List(column))

  /**
    * Get the provided columns for all rows in a table, filtered by the provided filter (can be Row or Column filter)
    *
    * @param columns A list of columns (consisting of column family and column)
    * @param filter  Filter or filter-list to apply to the scan
    * @param table   (implicit) The table to read from
    * @tparam K      Type of row key for which `RowKeyDecoder` needs to exist
    * @return List of Rows with each a row key and a mapping from column to value
    */
  def scan[K: RowKeyDecoder](
      columns: List[FullColumn],
      filter: Option[Filter] = None
    )(implicit table: Table
    ): List[Row[K]] = {
    val scan = createScan(columns = columns, filter = filter)
    execute(scan, columns)
  }

  /**
    * Get the provided column for all rows in a table, filtered by the provided filter (can be Row or Column filter)
    *
    * @param column Column with family
    * @param filter Filter or filter-list to apply to the value of the column c
    * @param table  (implicit) The table to read from
    * @tparam K     Type of row key for which `RowKeyDecoder` needs to exist
    * @return List of Rows with each a row key and a mapping from column to value
    */
  def scan[K: RowKeyDecoder](
      column: FullColumn,
      filter: Option[Filter]
    )(implicit table: Table
    ): List[Row[K]] =
    scan(List(column), filter)

  /**
    * Get the given column for all rows in a table
    *
    * @param column Column with family
    * @param table  (implicit) The table to read from
    * @tparam K     Type of row key for which `RowKeyDecoder` needs to exist
    * @return The values as a map of ( row key -> map ( column name -> value ) )
    */
  def scan[K: RowKeyDecoder](column: FullColumn)(implicit table: Table): List[Row[K]] = {
    val scan = createScan(columns = List(column))
    execute(scan, List(column))
  }

  /**
    * Get all rows with a given row prefix, only includes columns for the given column family
    *
    * @param rowKeyPrefix The prefix of the rows to scan
    * @param table        (implicit) The table to read from
    * @tparam P             Type of row key prefix for which `RowQueryEncoder` needs to exist
    * @tparam K             Type of row key for which `RowKeyDecoder` needs to exist
    * @tparam CF            Type of ColumnFamily
    * @tparam C             Type of Column that the factory function returns. Should have CF mixed in
    * @return The values
    */
  def scan[P: RowQueryEncoder, K: RowKeyDecoder, CF <: ColumnFamily, C <: Column with CF](
      rowKeyPrefix: P,
      columnFamily: CF,
      columnFactory: PartialFunction[String, C]
    )(implicit table: Table
    ): List[Row[K]] = {
    val scan =
      createRowPrefixScan(rowKeyPrefix = Some(rowKeyPrefix), columnFamily = Some(columnFamily))
    executeColumnFamily(scan, columnFamily, columnFactory)
  }

  /**
    * Get all rows with columns for the given column family from table.
    *
    * @param columnFamily The columnfamily name
    * @param table        (implicit) The table to read from
    * @tparam K           Type of row key for which `RowKeyDecoder` needs to exist
    * @tparam CF          Type of ColumnFamily
    * @tparam C           Type of Column that the factory function returns. Should have CF mixed in
    * @return The values as a map of ( row key -> map ( column name -> value ) )
    */
  def scan[K: RowKeyDecoder, CF <: ColumnFamily, C <: Column with CF](
      columnFamily: CF,
      columnFactory: PartialFunction[String, C]
    )(implicit table: Table
    ): List[Row[K]] = {
    val scan =
      createScan(columnFamily = Some(columnFamily))
    executeColumnFamily(scan, columnFamily, columnFactory)
  }

  /**
    * Create HBase Scan object to be executed against a table at a later time
    *
    * @param columns      A sequence of columns to filter on incl. column family. Result will only contain values
    *                     for these columns. If one or more columns are part of a column family specified in the
    *                     `columnFamily` parameter, these columns will be added instead of the entire column family
    * @param columnFamily A sequence of column families to filter on. Result will only contain values for these
    *                     column families
    * @param filter       HBase Filter to apply
    * @param minTimestamp Minimum timestamp to retrieve versions for, inclusive. Default: 0L
    * @param maxTimestamp Maximum timestamp to retrieve version for, exclusive. Default: Long.MAX_VALUE
    * @param versions     Number of versions to retrieve
    * @return             HBase Scan object
    */
  def createScan(
      columns: Seq[FullColumn] = Seq.empty,
      columnFamily: Option[ColumnFamily] = None,
      filter: Option[Filter] = None,
      minTimestamp: Option[Instant] = None,
      maxTimestamp: Option[Instant] = None,
      versions: Int = 1
    ): Scan = {
    val scan = new Scan()
    columnFamily.foreach(c => scan.addFamily(c.columnFamily))
    columns.foreach(c => scan.addColumn(c.columnFamily, c.name))
    filter.foreach { f =>
      scan.setFilter(f)
      checkFilter(scan, f)
    }
    calculateTimeRange(minTimestamp, maxTimestamp) match {
      case Some((minTS, maxTS)) => scan.setTimeRange(minTS, maxTS)
      case _                    =>
    }
    scan.readVersions(versions)
    scan
  }

  /**
    * Create HBase Scan object to be executed against a table at a later time
    *
    * @param rowKeyPrefix prefix of the row key to limit scanning range
    * @param columns      A sequence of columns to filter on incl. column family. Result will only contain values
    *                     for these columns. If one or more columns are part of a column family specified in the
    *                     `columnFamily` parameter, these columns will be added instead of the entire column family
    * @param columnFamily A sequence of column families to filter on. Result will only contain values for these
    *                     column families
    * @param filter       HBase Filter to apply
    * @param minTimestamp Minimum timestamp to retrieve versions for, inclusive. Default: 0L
    * @param maxTimestamp Maximum timestamp to retrieve version for, exclusive. Default: Long.MAX_VALUE
    * @param versions     Number of versions to retrieve
    * @tparam P           Type of row key prefix for which `RowQueryEncoder` needs to exist
    * @return             HBase Scan object
    */
  def createRowPrefixScan[P: RowQueryEncoder](
      rowKeyPrefix: Option[P] = None,
      columns: Seq[FullColumn] = Seq.empty,
      columnFamily: Option[ColumnFamily] = None,
      filter: Option[Filter] = None,
      minTimestamp: Option[Instant] = None,
      maxTimestamp: Option[Instant] = None,
      versions: Int = 1
    ): Scan = {
    val scan = createScan(columns, columnFamily, filter, minTimestamp, maxTimestamp, versions)
    rowKeyPrefix.foreach(p => scan.setRowPrefixFilter(p.encode))
    scan
  }

  /**
    * Execute HBase scan operation and return a list of Rows with the specified columns
    *
    * @param scan     HBase scan operation
    * @param columns  Columns to retrieve
    * @tparam K       Type of row key for which `RowQueryEncoder` needs to exist
    * @return List of rows with the specified columns
    */
  def execute[K: RowKeyDecoder](
      scan: Scan,
      columns: Seq[FullColumn]
    )(implicit table: Table
    ): List[Row[K]] = {
    val scanner = table.getScanner(scan)
    val results = scanner.asScala.toList
    results.map { result =>
      val rowKey = implicitly[RowKeyDecoder[K]].decode(result.getRow)
      val columnsValues = convertCellsToColumnsWithValues(result, columns)
      Row(rowKey, columnsValues)
    }
  }

  /**
    * Executes an HBase scan for a specific column family
    *
    * @param scan            HBase Scan operation
    * @param columnFamily   Column family for which the columns are to be retrieved
    * @param columnFactory  Factory function to instantiate FullColumn objects, takes a String that is the column name
    *                       as returned by HBase
    * @param table          (implicit) The HBase table to read from
    * @tparam K             Type of row key for which `RowKeyDecoder` needs to exist
    * @tparam CF            Type of ColumnFamily
    * @tparam C             Type of Column that the factory function returns. Should have CF mixed in
    * @return The Row with the columns of the requested column family if the row key exists, otherwise None
    */
  def executeColumnFamily[K: RowKeyDecoder, CF <: ColumnFamily, C <: Column with CF](
      scan: Scan,
      columnFamily: CF,
      columnFactory: PartialFunction[String, C]
    )(implicit table: Table
    ): List[Row[K]] = {
    val scanner = table.getScanner(scan)
    val results = scanner.asScala.toList
    results.map { result =>
      val rowKey = implicitly[RowKeyDecoder[K]].decode(result.getRow)
      val columnsValues = convertCellsToDynamicColumsWithValues(result, columnFamily, columnFactory)
      Row(rowKey, columnsValues)
    }
  }
}
