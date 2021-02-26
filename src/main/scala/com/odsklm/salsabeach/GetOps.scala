package com.odsklm.salsabeach

import java.time.Instant
import com.odsklm.salsabeach.types.ColumnDefs._
import com.odsklm.salsabeach.types.models.{Row, Value, VersionedRow}
import com.odsklm.salsabeach.types.rowconverters.{RowKeyDecoder, RowQueryEncoder}
import com.odsklm.salsabeach.types.rowconverters.RowQueryEncoder.ops._
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.hbase.client.{Get, Result, Table}
import org.apache.hadoop.hbase.filter.Filter

import scala.collection.SortedSet

object GetOps extends LazyLogging {

  /**
    * Get multiple columns from table from the row with the given row key.
    *
    * @param key        The row key
    * @param columns    A sequence of columns incl. column family
    * @param table      (implicit) The table to read from
    * @tparam K         Type of row key for which `RowQueryEncoder` needs to exist
    * @return The Row with the requested columns if the row key exists, otherwise None
    */
  def get[K: RowQueryEncoder](
      key: K,
      columns: Seq[FullColumn]
    )(implicit table: Table
    ): Option[Row[K]] = {
    val get = createGet(key, columns)
    execute(key, get, columns)
  }

  /**
    * Get multiple columns from table at a specific timestamp for a given row key
    *
    * @param key        The row key
    * @param columns    A sequence of columns incl. column family
    * @param timestamp  The moment for which the result is to be retrieved. HBase update timestamps of the result
    *                   will be at or before this timestamp.
    *                   NOTE: this behaviour has changed compared to previous versions of Salsa Beach, which used
    *                   `timestamp` exclusive instead of inclusive.
    *                   The new behaviour is more in line with that of the HBase Java client.
    * @param table      (implicit) The table to read from
    * @tparam K         Type of row key for which `RowQueryEncoder` needs to exist
    * @return The Row with the requested columns at a particular moment in time if the row key exist, otherwise None
    */
  def get[K: RowQueryEncoder](
      key: K,
      columns: Seq[FullColumn],
      timestamp: Instant
    )(implicit table: Table
    ): Option[Row[K]] = {
    val ts = Instant.ofEpochMilli(timestamp.toEpochMilli + 1)
    val get = createGet(key, columns, maxTimestamp = Some(ts))
    execute(key, get, columns)
  }

  /**
    * Get single column value from table for the given row key.
    *
    * @param key    The row key for the row to retrieve
    * @param column The column incl. column family
    * @param table  (implicit) The table to read from
    * @tparam K     Type of row key for which `RowQueryEncoder` needs to exist
    * @return The optional value. None is HBase returns `null` for the specified column or if the row key does not exist
    */
  def get[K: RowQueryEncoder](key: K, column: FullColumn)(implicit table: Table): Option[Value] = {
    val get = createGet(key, List(column))
    executeSingleColumn(get, column)
  }

  /**
    * Get all columns belonging to the given column family for the given row key.
    *
    * @param key            The row key
    * @param columnFamily   The columnfamily name
    * @param columnFactory  Factory function to create FullColumn objects from the column names as returned by HBase
    * @param filter         Optional filter
    * @param table          (implicit) The table to read from
    * @tparam K             Type of row key for which `RowQueryEncoder` needs to exist
    * @tparam CF            Type of ColumnFamily
    * @tparam C             Type of Column that the factory function returns. Should have CF mixed in
    * @return The Row with the columns of the requested column family if the row key exists, otherwise None
    */
  def get[K: RowQueryEncoder, CF <: ColumnFamily, C <: Column with CF](
      key: K,
      columnFamily: CF,
      columnFactory: PartialFunction[String, C],
      filter: Option[Filter] = None
    )(implicit table: Table
    ): Option[Row[K]] = {
    val get = createGet(key, columnFamily = Some(columnFamily), filter = filter)
    executeColumnFamily(key, get, columnFamily, columnFactory)
  }

  /**
    * Get multiple versions of a single column from a row.
    *
    * @param key          The row key
    * @param column       The column incl. column family
    * @param versions     The maximum number of versions to return, defaults to all
    * @param minTimestamp Minimum timestamp to retrieve versions for, inclusive. Default: 0L
    * @param maxTimestamp Maximum timestamp to retrieve version for, exclusive. Default: Long.MAX_VALUE
    * @param table        (implicit) The table to read from
    * @tparam K           Type of row key for which `RowQueryEncoder` needs to exist
    * @return Sorted set with values if the row key exists, otherwise None
    */
  def getVersionsForColumn[K: RowQueryEncoder](
      key: K,
      column: FullColumn,
      versions: Int = Int.MaxValue,
      minTimestamp: Option[Instant] = None,
      maxTimestamp: Option[Instant] = None
    )(implicit table: Table
    ): Option[SortedSet[Value]] = {
    val get = createGet(
      key,
      List(column),
      minTimestamp = minTimestamp,
      maxTimestamp = maxTimestamp,
      versions = versions
    )
    executeVersionedSingleColumn(get, column)
  }

  /**
    * Get multiple versions for the given columns as a Row.
    *
    * @param key            The row key
    * @param columns        A sequence of columns incl. column family
    * @param versions       The maximum number of versions to return, defaults to all
    * @param minTimestamp   Minimum timestamp to retrieve versions for, inclusive. Default: 0L
    * @param maxTimestamp   Maximum timestamp to retrieve version for, exclusive. Default: Long.MAX_VALUE
    * @param table          (implicit) The table to read from
    * @return               The values as a VersionedRow if the row key exists, otherwise None
    */
  def getVersions[P: RowQueryEncoder, K: RowKeyDecoder](
      key: P,
      columns: Seq[FullColumn],
      versions: Int = Int.MaxValue,
      minTimestamp: Option[Instant] = None,
      maxTimestamp: Option[Instant] = None
    )(implicit table: Table
    ): Option[VersionedRow[K]] = {
    val get = createGet(
      key,
      columns,
      minTimestamp = minTimestamp,
      maxTimestamp = maxTimestamp,
      versions = versions
    )
    executeVersioned(get, columns)
  }

  /**
    * Create HBase Get object to be executed against a table at a later time
    *
    * @param key            The row key
    * @param columns        A sequence of columns to filter on incl. column family. Result will only contain values
    *                       for these columns. If one or more columns are part of a column family specified in the
    *                       `columnFamily` parameter, these columns will be added instead of the entire column family
    * @param columnFamily   A sequence of column families to filter on. Result will only contain values for these
    *                       column families.
    * @param filter         HBase Filter to apply
    * @param minTimestamp   Minimum timestamp to retrieve versions for, inclusive. Default: 0L
    * @param maxTimestamp   Maximum timestamp to retrieve version for, exclusive. Default: Long.MAX_VALUE
    * @param versions       Number of versions to retrieve
    * @tparam K             Type of row key for which `RowQueryEncoder` needs to exist
    * @return               HBase Get object
    */
  def createGet[K: RowQueryEncoder](
      key: K,
      columns: Seq[FullColumn] = Seq.empty,
      columnFamily: Option[ColumnFamily] = None,
      filter: Option[Filter] = None,
      minTimestamp: Option[Instant] = None,
      maxTimestamp: Option[Instant] = None,
      versions: Int = 1
    ): Get = {
    val get = new Get(key.encode)
    columnFamily.foreach(c => get.addFamily(c.columnFamily))
    columns.foreach(c => get.addColumn(c.columnFamily, c.name))
    filter.foreach(f => get.setFilter(f))
    calculateTimeRange(minTimestamp, maxTimestamp) match {
      case Some((minTS, maxTS)) => get.setTimeRange(minTS, maxTS)
      case _                    =>
    }
    get.readVersions(versions)
    get
  }

  /**
    * Executes an HBase Get operation with multiple columns to retrieve and returns a Row
    *
    * @param key      Row key associated with Get. NOTE: this function will not check if `key` corresponds with the key that
    *                 get was constructed with!
    * @param get      HBase Get operation
    * @param columns  Columns to retrieve, needed for translating HBase Result back to Salsa Beach Row
    * @param table    (implicit) The HBase table to read from
    * @tparam K       Type of row key for which `RowQueryEncoder` needs to exist
    * @return Row object with columns and corresponding values or None if the row does not exist. If no value exists
    *         for column, that column and value are left out of the result. Use `Row.getValue()` to safely retrieve
    *         a possibly non-existent value
    */
  def execute[K: RowQueryEncoder](
      key: K,
      get: Get,
      columns: Seq[FullColumn]
    )(implicit table: Table
    ): Option[Row[K]] = {
    val result = getOptionalResult(get)
    result.map(r => convertCellsToColumnsWithValues(r, columns)).map(Row(key, _))
  }

  /**
    * Executes an HBase Get operation with a single column to retrieve and returns an optional Value
    *
    * @param get    HBase Get operation
    * @param column Column to retrieve
    * @param table  (implicit) The HBase table to read from
    * @return Optional Value, which is None if HBase Get operation returns `null` for the specified column
    *         or if the row does not exist
    */
  def executeSingleColumn(get: Get, column: FullColumn)(implicit table: Table): Option[Value] = {
    val result = getOptionalResult(get)
    result.map(_.getColumnLatestCell(column.columnFamily, column.name)).flatMap(cell2OptionalValue)
  }

  /**
    * Executes an HBase Get operation with multiple versions of multiple columns to retrieve and returns a versioned
    * row
    *
    * @param get      HBase Get operation
    * @param columns  Columns to retrieve, needed for translating HBase Result back to Salsa Beach VersionedRow
    * @param table    (implicit) The HBase table to read from
    * @tparam K       Type of row key for which `RowKeyDecoder` needs to exist
    * @return         VersionedRow object with columns and corresponding versioned values. If no value exists for
    *                 column, that column and value versions are left out of the result.
    *                 Use `VersionedRow.getLatestValue()` or `VersionedRow.getValueAt()` to safely retrieve a possibly
    *                 non-existent value
    */
  def executeVersioned[K: RowKeyDecoder](
      get: Get,
      columns: Seq[FullColumn]
    )(implicit table: Table
    ): Option[VersionedRow[K]] = {
    val result = getOptionalResult(get)
    result.map { r =>
      val rowKey = implicitly[RowKeyDecoder[K]].decode(r.getRow)
      val versionedColumns = convertCellsToColumnsWithVersionedValues(r, columns)
      VersionedRow(rowKey, versionedColumns)
    }
  }

  /**
    * Executes an HBase Get operation with multiple versions of a single column to retrieve and returns a sorted set
    * of Values
    *
    * @param get    HBase Get operation
    * @param column Column to retrieve
    * @param table  (implicit) The HBase table to read from
    * @return SortedSet of Values for the specified column
    */
  def executeVersionedSingleColumn(
      get: Get,
      column: FullColumn
    )(implicit table: Table
    ): Option[SortedSet[Value]] = {
    val result = getOptionalResult(get)
    result.map(convertCellsToVersionedValues(_, column))
  }

  /**
    * Executes an HBase get for row and column family
    *
    * @param key            Row key associated with Get. NOTE: this function will not check if `key` corresponds with the key that
    *                       get was constructed with!
    * @param get            HBase Get operation
    * @param columnFamily   Column family for which the columns are to be retrieved
    * @param columnFactory  Factory function to instantiate FullColumn objects, takes a String that is the column name
    *                       as returned by HBase
    * @param table          (implicit) The HBase table to read from
    * @tparam K             Type of row key for which `RowQueryEncoder` needs to exist
    * @tparam CF            Type of ColumnFamily
    * @tparam C             Type of Column that the factory function returns. Should have CF mixed in
    * @return The Row with the columns of the requested column family if the row key exists, otherwise None
    */
  def executeColumnFamily[K: RowQueryEncoder, CF <: ColumnFamily, C <: Column with CF](
      key: K,
      get: Get,
      columnFamily: CF,
      columnFactory: PartialFunction[String, C]
    )(implicit table: Table
    ): Option[Row[K]] = {
    val result = getOptionalResult(get)
    result
      .map(convertCellsToDynamicColumsWithValues(_, columnFamily, columnFactory))
      .map(Row(key, _))
  }

  private def getOptionalResult(get: Get)(implicit table: Table): Option[Result] = {
    val result = table.get(get)
    if (result.getRow == null)
      None
    else
      Some(result)
  }
}
