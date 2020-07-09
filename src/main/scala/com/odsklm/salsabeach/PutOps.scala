package com.odsklm.salsabeach

import java.time.Instant

import com.odsklm.salsabeach.types.ColumnDefs._
import com.odsklm.salsabeach.types.rowconverters.RowQueryEncoder
import com.odsklm.salsabeach.types.rowconverters.RowQueryEncoder.ops._
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.hbase.client.{Put, Table}

object PutOps extends LazyLogging {

  /**
    * Store/replace data into a row of the given HBase table. Empty column values are ignored.
    *
    * @param key        The row key
    * @param columnVals A map containing FullColumn -> value
    * @param table      (implicit) The table to update
    */
  def put[K: RowQueryEncoder](
      key: K,
      columnVals: Map[FullColumn, Option[String]],
      timestamp: Option[Instant] = None
    )(implicit table: Table
    ): Unit = {

    val put = new Put(key.encode)

    columnVals.foreach {
      case (col, Some(value)) =>
        timestamp match {
          case Some(ts) => put.addColumn(col.columnFamily, col.name, ts.toEpochMilli, value)
          case _        => put.addColumn(col.columnFamily, col.name, value)
        }
      case (_, None) => // no-op
    }
    if (!put.getFamilyCellMap.isEmpty) table.put(put)
  }

  /**
    * Store/replace data into a row of the given HBase table. Empty column values are ignored.
    *
    * @param key          The row key
    * @param columnFamily Column family
    * @param columnVals   A map containing column as Byte Array -> value as optional Byte Array
    * @param table        (implicit) The table to update
    */
  def putRaw(
      key: Array[Byte],
      columnFamily: ColumnFamily,
      columnVals: Map[Array[Byte], Option[Array[Byte]]]
    )(implicit table: Table
    ): Unit = {

    val put = new Put(key)

    columnVals.foreach {
      case (col, Some(value)) =>
        put.addColumn(columnFamily.columnFamily, col, value)
      case (_, None) => // no-op
    }
    if (!put.getFamilyCellMap.isEmpty) table.put(put)
  }
}
