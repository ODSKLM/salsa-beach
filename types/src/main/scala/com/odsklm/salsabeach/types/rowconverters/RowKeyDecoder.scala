package com.odsklm.salsabeach.types.rowconverters

trait RowKeyDecoder[A] {
  def decode(rk: Array[Byte]): A
}

object RowKeyDecoder {
  def apply[A](decode: Array[Byte] => A): RowKeyDecoder[A] = (rk: Array[Byte]) => decode(rk)

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
