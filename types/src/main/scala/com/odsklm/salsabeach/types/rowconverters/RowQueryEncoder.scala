package com.odsklm.salsabeach.types.rowconverters

/**
  * RowQueryEncoder is a type class that lets you translate any type into a byte string that can be used to find one
  * or more rows in HBase. It is used for all HBase operations: get, put, scan, delete
  * @tparam A type to encode
  */
trait RowQueryEncoder[A] {
  def encode(instance: A): Array[Byte]
}

object RowQueryEncoder {
  def apply[A](encodeFn: A => Array[Byte]): RowQueryEncoder[A] = new RowQueryEncoder[A] {
    override def encode(instance: A): Array[Byte] = encodeFn(instance)
  }

  object instances {
    implicit val utf8StringRowKeyEncoder: RowQueryEncoder[String] =
      RowQueryEncoder((s: String) => s.getBytes(UTF8_CHARSET))
    implicit val intRowKeyEncoder: RowQueryEncoder[Int] =
      RowQueryEncoder((i: Int) => BigInt(i).toByteArray)
  }

  object ops {
    def encode[A: RowQueryEncoder](instance: A): Array[Byte] =
      implicitly[RowQueryEncoder[A]].encode(instance)

    implicit class RowKeyEncoderOps[A: RowQueryEncoder](instance: A) {
      def encode: Array[Byte] = implicitly[RowQueryEncoder[A]].encode(instance)
    }
  }
}
