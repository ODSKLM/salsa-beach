package com.odsklm.salsabeach.types.rowconverters

import com.odsklm.salsabeach.types.rowconverters.RowKeyDecoder.instances._
import com.odsklm.salsabeach.types.rowconverters.RowKeyDecoder.ops._
import com.odsklm.salsabeach.types.rowconverters.fixtures.Person
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RowKeyDecoderSpec extends AnyFlatSpec with Matchers {

  "decode" should "work when called as a function" in {
    decode[String]("test".getBytes("UTF-8")) shouldBe "test"
  }

  "decode" should "work when called on object" in {
    val b = "test".getBytes("UTF-8")
    b.decode[String] shouldBe "test"
  }

  "Conversions" should "work for Int using default implementation" in {
    BigInt(1).toByteArray.decode[Int] shouldBe 1
  }

  implicit val personRowKeyDecoder: RowKeyDecoder[Person] = RowKeyDecoder[Person] {
    (b: Array[Byte]) =>
      val segments = new String(b, "UTF-8").split("\\|")
      Person(segments(0), segments(1).toInt)
  }

  "Conversions" should "work for custom implementations" in {
    "John|30".getBytes("UTF-8").decode[Person] shouldBe Person("John", 30)
  }
}
