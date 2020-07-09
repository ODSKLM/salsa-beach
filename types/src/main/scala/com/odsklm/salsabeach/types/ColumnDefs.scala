package com.odsklm.salsabeach.types

object ColumnDefs {
  type FullColumn = Column with ColumnFamily
  implicit val fullColumnOrdering: Ordering[FullColumn] = Ordering.by(_.name)

  trait ColumnFamily {
    val columnFamily: String
  }

  trait Column {
    this: ColumnFamily =>

    // Only calculate this once when the inherited class asks about it
    private lazy val classAsSnakeCase = this
      .getClass
      .getSimpleName
      .replaceAll("[$]", "")
      .replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2")
      .replaceAll("([a-z\\d])([A-Z])", "$1_$2")
      .toLowerCase

    def name: String = classAsSnakeCase

    // Prevent hashCode collision when the column has the same name, but in different families
    override def hashCode(): Int = (columnFamily + name).hashCode
  }
}
