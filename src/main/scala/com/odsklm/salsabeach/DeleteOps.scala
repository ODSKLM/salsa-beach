package com.odsklm.salsabeach

import com.odsklm.salsabeach.types.ColumnDefs._
import com.odsklm.salsabeach.types.rowconverters.RowQueryEncoder
import com.odsklm.salsabeach.types.rowconverters.RowQueryEncoder.ops._
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.hbase.client.{Delete, Table}

object DeleteOps extends LazyLogging {

  /**
    * Delete the latest version of a single column for the given key/column from the table.
    *
    * @param key    The row key
    * @param column The column to delete
    * @param table  (implicit) The table to delete the columns from
    */
  def delete[K: RowQueryEncoder](key: K, column: FullColumn)(implicit table: Table): Unit =
    delete(key, List(column))

  /**
    * Delete the latest version of a number of columns for the given key from the table.
    *
    * @param key     The row key
    * @param columns The columns to delete
    * @param table   (implicit) The table to delete the columns from
    */
  def delete[K: RowQueryEncoder](key: K, columns: List[FullColumn])(implicit table: Table): Unit = {
    val delete = new Delete(key.encode)
    columns.foreach(c => delete.addColumn(c.columnFamily, c.name))
    table.delete(delete)
  }

  /**
    * Delete all versions of all columns for the given key and column family from the table.
    *
    * @param key          The row key
    * @param columnFamily The column family of which to delete all columns
    * @param table        (implicit) The table to delete the columns from
    */
  def delete[K: RowQueryEncoder](key: K, columnFamily: ColumnFamily)(implicit table: Table): Unit = {
    val delete = new Delete(key.encode)
    delete.addFamily(columnFamily.columnFamily)
    table.delete(delete)
  }

  /**
    * Delete the complete row for the given key
    *
    * @param key    The row key
    * @param table  (implicit) The table to delete the columns from
    */
  def delete[K: RowQueryEncoder](key: K)(implicit table: Table): Unit = {
    val delete = new Delete(key.encode)
    table.delete(delete)
  }
}
