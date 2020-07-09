package com.odsklm.salsabeach.types.rowconverters

/**
  * RowKeyDecoder is a type class for decoding row keys, represented as byte arrays, into concrete types
  * @tparam A type to decode the row keys to
  */
trait RowKeyDecoder[A] {
  def decode(rowKey: Array[Byte]): A
}

object RowKeyDecoder {
  def apply[A](decodeFn: Array[Byte] => A): RowKeyDecoder[A] = new RowKeyDecoder[A] {
    override def decode(rowKey: Array[Byte]): A = decodeFn(rowKey)
  }

  object ops {
    def decode[A: RowKeyDecoder](rk: Array[Byte]): A =
      implicitly[RowKeyDecoder[A]].decode(rk)

    implicit class RowKeyDecoderOps(rk: Array[Byte]) {
      def decode[A: RowKeyDecoder]: A = implicitly[RowKeyDecoder[A]].decode(rk)
    }
  }

  object instances {
    implicit val utf8StringRowKeyDecoder: RowKeyDecoder[String] =
      RowKeyDecoder[String]((b: Array[Byte]) => new String(b, UTF8_CHARSET))
    implicit val intRowKeyDecoder: RowKeyDecoder[Int] =
      RowKeyDecoder[Int]((b: Array[Byte]) => BigInt(b).toInt)
  }
}
