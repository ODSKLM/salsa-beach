package com.odsklm.salsabeach.types.rowconverters

import com.odsklm.salsabeach.types.rowconverters.RowQueryEncoder.instances._
import com.odsklm.salsabeach.types.rowconverters.RowQueryEncoder.ops._
import com.odsklm.salsabeach.types.rowconverters.fixtures.Person
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RowQueryEncoderSpec extends AnyFlatSpec with Matchers {
  "encode" should "work when called as a function" in {
    encode("test") shouldBe "test".getBytes("UTF-8")
  }

  "encode" should "work when called on object" in {
    "test".encode shouldBe "test".getBytes("UTF-8")
  }

  "Conversions" should "work for Int using default implementation" in {
    1.encode shouldBe BigInt(1).toByteArray
  }

  implicit val personRowKeyEncoder: RowQueryEncoder[Person] =
    RowQueryEncoder[Person]((p: Person) => s"${p.name}|${p.age}".getBytes("UTF-8"))

  "Conversions" should "work for custom implementations" in {
    val p = Person("John", 30)
    p.encode shouldBe "John|30".getBytes("UTF-8")
  }
}
