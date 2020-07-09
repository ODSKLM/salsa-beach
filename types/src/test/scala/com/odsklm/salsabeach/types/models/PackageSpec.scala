package com.odsklm.salsabeach.types.models

import com.odsklm.salsabeach.types.models.fixtures._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.SortedSet

class PackageSpec extends AnyFlatSpec with Matchers {

  "valueOrdering" should "order on timestamp descending first, then on value" in {
    val values = List(value3, value2, value4, value1)
    val sortedValues = SortedSet(values: _*)
    val expectedOrdering = List(value4, value2, value3, value1)
    sortedValues.toList shouldEqual expectedOrdering
  }
}
