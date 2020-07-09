package com.odsklm.salsabeach.types.models

import java.time.Instant

import com.odsklm.salsabeach.types.ColumnDefs.{Column, ColumnFamily}
import com.odsklm.salsabeach.types.models.fixtures._
import com.odsklm.salsabeach.types.rowconverters.RowQueryEncoder.instances.utf8StringRowKeyEncoder
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.SortedSet

class VersionedRowSpec extends AnyFlatSpec with Matchers with OptionValues {

  private trait TestCF extends ColumnFamily {
    override val columnFamily: String = "test_cf"
  }
  private trait TestCol extends Column with TestCF
  private object TestCol1 extends TestCol
  private object TestCol2 extends TestCol
  private object TestCol3 extends TestCol
  private object TestCol4 extends TestCol

  private val columnValues: ColumnsValueVersions = Map(
    TestCol1 -> SortedSet(value1, value3, value4),
    TestCol2 -> SortedSet.empty,
    TestCol3 -> SortedSet(value4)
  )

  private val row = VersionedRow("row1", columnValues)

  "getValueAt" should "get the latest value at or before timestamp" in {
    row.getValueAt(TestCol1, Instant.now).value shouldBe value4
    row.getValueAt(TestCol1, afternoon1PM).value shouldBe value3
    row.getValueAt(TestCol1, afternoon2PM).value shouldBe value3
    row.getValueAt(TestCol2, Instant.now) shouldBe None
    row.getValueAt(TestCol3, afternoon1PM) shouldBe None
    row.getValueAt(TestCol3, Instant.now).value shouldBe value4
    row.getValueAt(TestCol4, Instant.now) shouldBe None
  }

  "getLatestValue" should "get the latest value for a column" in {
    row.getLatestValue(TestCol1).value shouldBe value4
    row.getLatestValue(TestCol2) shouldBe None
    row.getLatestValue(TestCol3).value shouldBe value4
    row.getLatestValue(TestCol4) shouldBe None
  }

  "getLatestRowValues" should "return the latest version for each column if a version exists" in {
    val latestRow = Row("row1", Map(TestCol1 -> value4, TestCol3 -> value4))
    row.getLatestRow shouldBe latestRow
  }

  "getRowValuesAt" should "return the value of each column at a certain timestamp if a value exists" in {
    val rowAt2PM = Row("row1", Map(TestCol1 -> value3))
    val rowNow = Row("row1", Map(TestCol1 -> value4, TestCol3 -> value4))
    row.getRowAt(afternoon2PM) shouldBe rowAt2PM
    row.getRowAt(Instant.now) shouldBe rowNow
  }
}
