package com.odsklm.salsabeach.types.models

import com.odsklm.salsabeach.types.ColumnDefs.FullColumn

case class Row[A](rowKey: A, columnsValues: ColumnsValues) {
  def getValue(column: FullColumn): Option[Value] =
    columnsValues.get(column)
}
