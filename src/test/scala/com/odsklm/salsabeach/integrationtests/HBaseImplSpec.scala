package com.odsklm.salsabeach.integrationtests

import java.time.{Instant, LocalDate}
import java.time.format.DateTimeFormatter

import com.odsklm.salsabeach.DeleteOps._
import com.odsklm.salsabeach.GetOps._
import com.odsklm.salsabeach.HBase.{createOrUpdateTable, truncate, withConnection, withTable}
import com.odsklm.salsabeach.PutOps._
import com.odsklm.salsabeach.ScanOps._
import com.odsklm.salsabeach.integrationtests.facilities.{HBaseEnabled, HBaseMatchers}
import com.odsklm.salsabeach.types.ColumnDefs._
import com.odsklm.salsabeach.types.models.{Row, Value}
import com.odsklm.salsabeach.types.rowconverters.{RowKeyDecoder, RowQueryEncoder}
import com.odsklm.salsabeach.types.rowconverters.RowQueryEncoder.ops._
import org.apache.hadoop.hbase.CompareOperator
import org.apache.hadoop.hbase.filter.{BinaryComparator, CompareFilter, FilterList, SingleColumnValueFilter}
import org.apache.hadoop.hbase.util.Bytes
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, OptionValues}

import scala.collection.SortedSet

// TODO: Split in different specs for GetOps, ScanOps, PutOps and DeleteOps
// This requires setting up the HBase test cluster in such a way that it can be reused between tests and will not
// be started more than once
class HBaseImplSpec
    extends AnyFlatSpec
    with HBaseEnabled
    with BeforeAndAfterAll
    with Matchers
    with OptionValues
    with HBaseMatchers {

  private val testTable = "test_table"

  private trait TestCF1 extends ColumnFamily {
    override val columnFamily: String = "test_cf1"
  }

  private trait TestCF2 extends ColumnFamily {
    override val columnFamily: String = "test_cf2"
  }

  private object TestCF1 extends TestCF1
  private object TestCF2 extends TestCF2

  private trait TestCF1Col extends Column with TestCF1
  private object TestCol1 extends TestCF1Col
  private object TestCol2 extends TestCF1Col

  private trait TestCF2Col extends Column with TestCF2
  private object TestCol3 extends TestCF2Col

  private case class TestRowKey(date: LocalDate, name: String)
  private case class TestRowKeyPrefix(date: LocalDate)

  private val iso = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  implicit private val testRowKeyEncoder: RowQueryEncoder[TestRowKey] =
    RowQueryEncoder[TestRowKey] { (key: TestRowKey) =>
      val date = key.date.format(iso)
      s"$date|${key.name}".getBytes("UTF-8")
    }

  implicit private val testRowKeyPrefixEncoder: RowQueryEncoder[TestRowKeyPrefix] =
    RowQueryEncoder[TestRowKeyPrefix] { (key: TestRowKeyPrefix) =>
      val date = key.date.format(iso)
      s"$date".getBytes("UTF-8")
    }

  implicit private val testRowKeyDecoder: RowKeyDecoder[TestRowKey] = RowKeyDecoder[TestRowKey] {
    (b: Array[Byte]) =>
      val segments = new String(b, "UTF-8").split("\\|")
      TestRowKey(LocalDate.parse(segments(0), iso), segments(1))
  }

  private val testDate = LocalDate.of(2017, 1, 1)
  private val testKey1 = TestRowKey(testDate, "test")
  private val testKey2 = TestRowKey(testDate, "test2")

  implicit class RowSeqOps[A](rows: Seq[Row[A]]) {
    def select(rowKey: A): Row[A] = rows.find(_.rowKey == rowKey).value
  }


  override def beforeAll {
    super.beforeAll()

    // In theory we should test this as well, but on the other hand, if this fails, the test suite fails, so we'll
    // know anyways
    withConnection { implicit conn =>
      createOrUpdateTable(testTable, List(TestCF1, TestCF2), 3)
    }
  }

  override def afterAll() {
    super.afterAll()
  }

  it should "put some data into HBase, and get it out of there" in {
    withConnection { implicit conn =>
      withTable(testTable) { implicit table =>
        put(
          testKey1,
          Map(
            TestCol1 -> Some("valueA"),
            TestCol2 -> Some("valueB")
          )
        )
      }

      // Get a single column
      val value = withTable(testTable) { implicit table =>
        get(testKey1, TestCol1)
      }
      value should matchOptionalValue("valueA")

      // Get multiple columns
      val row = withTable(testTable) { implicit table =>
        get(testKey1, List(TestCol1, TestCol2))
      }

      row shouldBe defined
      row.value.getValue(TestCol1) should matchOptionalValue("valueA")
      row.value.getValue(TestCol2) should matchOptionalValue("valueB")
    }
  }

  it should "be able perform a put with no items to be added" in {
    withConnection { implicit conn =>
      withTable(testTable) { implicit table =>
        put(
          testKey2,
          Map(
            TestCol1 -> None,
            TestCol2 -> None
          )
        )
      }

      val value = withTable(testTable) { implicit table =>
        get(testKey2, TestCol1)
      }
      value shouldBe empty
    }
  }

  it should "get all rows for the given columns" in {
    val testRowKey1 = TestRowKey(testDate, "test001")
    val testRowKey2 = TestRowKey(testDate, "test002")

    withConnection { implicit conn =>
      withTable(testTable) { implicit table =>
        put(
          testRowKey1,
          Map(
            TestCol1 -> Some("valueRow1Col1")
          )
        )
        put(
          testRowKey2,
          Map(
            TestCol1 -> Some("valueRow2Col1"),
            TestCol2 -> Some("valueRow2Col2")
          )
        )
      }

      val rows = withTable(testTable) { implicit table =>
        scan(TestCol1)
      }

      rows should containRowWithKey(testRowKey1)
      val row1 = rows select testRowKey1
      row1.columnsValues should contain key TestCol1
      row1.columnsValues(TestCol1).value shouldBe "valueRow1Col1"
      rows should containRowWithKey(testRowKey2)
      val row2 = rows select testRowKey2
      row2.columnsValues should contain key TestCol1
      row2.columnsValues(TestCol1).value shouldBe "valueRow2Col1"
      row2.columnsValues should not contain key(TestCol2)
    }
  }

  it should "get all rows for a given column family" in {
    val testRowKey1 = TestRowKey(testDate, "test001")
    val testRowKey2 = TestRowKey(testDate, "test002")

    case class TestCF1DynColumn(override val name: String) extends Column with TestCF1
    val testCol1Dynamic = TestCF1DynColumn("test_col1")
    val testCol2Dynamic = TestCF1DynColumn("test_col2")
    val testCol3Dynamic = TestCF1DynColumn("test_col3")
    val columnFactory: PartialFunction[String, TestCF1DynColumn] =  {
      case (name: String) => TestCF1DynColumn(name)
    }

    withConnection { implicit conn =>
      withTable(testTable) { implicit table =>
        put(
          testRowKey1,
          Map(
            TestCol1 -> Some("valueRow1Col1"),
            TestCol2 -> Some("valueRow1Col2")
          )
        )
        put(
          testRowKey2,
          Map(
            TestCol1 -> Some("valueRow2Col1"),
            TestCol3 -> Some("valueRow2Col3")
          )
        )
      }

      val rows = withTable(testTable) { implicit table =>
        scan[TestRowKey, TestCF1, TestCF1DynColumn](TestCF1, columnFactory)
      }

      rows should containRowWithKey(testRowKey1)
      rows should containRowWithKey(testRowKey2)
      val row1 = rows select testRowKey1
      val row2 = rows select testRowKey2
      row1.columnsValues should contain key testCol1Dynamic
      row1.columnsValues should contain key testCol2Dynamic
      row1.columnsValues(testCol1Dynamic).value shouldBe "valueRow1Col1"
      row1.columnsValues(testCol2Dynamic).value shouldBe "valueRow1Col2"
      row2.columnsValues should contain key testCol1Dynamic
      row2.columnsValues should not contain key(testCol3Dynamic)
      row2.columnsValues(testCol1Dynamic).value shouldBe "valueRow2Col1"
    }
  }

  it should "get all rows for a column with a given row prefix" in {
    val testRowKey1 = TestRowKey(LocalDate.of(2017, 2, 1), "test1")
    val testRowKey2 = TestRowKey(LocalDate.of(2017, 2, 1), "test2")
    val testRowKey3 = TestRowKey(LocalDate.of(2017, 2, 2), "test3")
    withConnection { implicit conn =>
      withTable(testTable) { implicit table =>
        put(
          testRowKey1,
          Map(
            TestCol1 -> Some("valueRow1Col1")
          )
        )
        put(
          testRowKey2,
          Map(
            TestCol1 -> Some("valueRow2Col1")
          )
        )
        put(
          testRowKey3,
          Map(
            TestCol1 -> Some("valueRow3Col1")
          )
        )
      }

      val rowPrefix1 = TestRowKeyPrefix(LocalDate.of(2017, 2, 1))
      val rows1 = withTable(testTable) { implicit table =>
        scan(rowPrefix1, TestCol1)
      }

      rows1 should containRowWithKey(testRowKey1)
      rows1 should containRowWithKey(testRowKey2)
      rows1 shouldNot containRowWithKey(testRowKey3)
      val row1 = rows1 select testRowKey1
      val row2 = rows1 select testRowKey2
      row1.columnsValues should contain key TestCol1
      row1.columnsValues(TestCol1).value shouldBe "valueRow1Col1"
      row2.columnsValues should contain key TestCol1
      row2.columnsValues(TestCol1).value shouldBe "valueRow2Col1"

      val rowPrefix2 = TestRowKeyPrefix(LocalDate.of(2017, 2, 2))
      val rows2 = withTable(testTable) { implicit table =>
        scan(rowPrefix2, TestCol1)
      }

      rows2 shouldNot containRowWithKey(testRowKey1)
      rows2 shouldNot containRowWithKey(testRowKey2)
      rows2 should containRowWithKey(testRowKey3)
      val row3 = rows2 select testRowKey3
      row3.columnsValues should contain key TestCol1
      row3.columnsValues(TestCol1).value shouldBe "valueRow3Col1"
    }
  }

  it should "get all rows for a given column with a given column filter" in {
    def testKey(name: String) = TestRowKey(LocalDate.of(2017, 5, 10), name)
    val testKeyPPP = testKey("PPP")
    val testKeyQQQ = testKey("QQQ")
    val testKeyRRR = testKey("RRR")
    val testKeySSS = testKey("SSS")
    val testKeyTTT = testKey("TTT")
    val testKeyUUU = testKey("UUU")

    withConnection { implicit conn =>
      withTable(testTable) { implicit table =>
        put(testKeyPPP, Map(TestCol1 -> Some("abc")))
        put(testKeyQQQ, Map(TestCol1 -> Some("def")))
        put(testKeyRRR, Map(TestCol1 -> Some("deg")))
        put(testKeySSS, Map(TestCol1 -> Some("deq")))
        put(testKeyTTT, Map(TestCol1 -> Some("der")))
        put(testKeyUUU, Map(TestCol1 -> Some("xyz")))
      }

      val filter1 = new SingleColumnValueFilter(
        Bytes.toBytes(TestCol1.columnFamily),
        Bytes.toBytes(TestCol1.name),
        CompareOperator.GREATER_OR_EQUAL,
        new BinaryComparator(Bytes.toBytes("deg"))
      )
      val filter2 = new SingleColumnValueFilter(
        Bytes.toBytes(TestCol1.columnFamily),
        Bytes.toBytes(TestCol1.name),
        CompareOperator.LESS,
        new BinaryComparator(Bytes.toBytes("der"))
      )
      val filter3 = new FilterList(FilterList.Operator.MUST_PASS_ALL)
      filter3.addFilter(filter1)
      filter3.addFilter(filter2)

      val rows1 = withTable(testTable) { implicit table =>
        scan(TestCol1, Some(filter1))
      }
      val rows2 = withTable(testTable) { implicit table =>
        scan(TestCol1, Some(filter2))
      }
      val rows3 = withTable(testTable) { implicit table =>
        scan(TestCol1, Some(filter3))
      }

      rows1 shouldNot containRowWithKey(testKeyPPP)
      rows1 shouldNot containRowWithKey(testKeyQQQ)
      rows1 should containRowWithKey(testKeyRRR)
      rows1 should containRowWithKey(testKeySSS)
      rows1 should containRowWithKey(testKeyTTT)
      rows1 should containRowWithKey(testKeyUUU)

      rows2 should containRowWithKey(testKeyPPP)
      rows2 should containRowWithKey(testKeyQQQ)
      rows2 should containRowWithKey(testKeyRRR)
      rows2 should containRowWithKey(testKeySSS)
      rows2 shouldNot containRowWithKey(testKeyTTT)
      rows2 shouldNot containRowWithKey(testKeyUUU)

      rows3 shouldNot containRowWithKey(testKeyPPP)
      rows3 shouldNot containRowWithKey(testKeyQQQ)
      rows3 should containRowWithKey(testKeyRRR)
      rows3 should containRowWithKey(testKeySSS)
      rows3 shouldNot containRowWithKey(testKeyTTT)
      rows3 shouldNot containRowWithKey(testKeyUUU)
    }
  }

  it should "delete one or more columns from a given row" in {
    def testKey(name: String) = TestRowKey(LocalDate.of(2017, 4, 4), name)

    val testKey1 = testKey("row1")
    val testKey2 = testKey("row2")

    withConnection { implicit conn =>
      withTable(testTable) { implicit table =>
        put(
          testKey1,
          Map(
            TestCol1 -> Some("valueRow1Col1"),
            TestCol2 -> Some("valueRow1Col2")
          )
        )
        put(
          testKey2,
          Map(
            TestCol1 -> Some("valueRow2Col1"),
            TestCol2 -> Some("valueRow2Col2")
          )
        )
      }
      val rowsBefore = withTable(testTable) { implicit table =>
        scan(List(TestCol1, TestCol2))
      }

      rowsBefore should containRowWithKey(testKey1)
      val row1 = rowsBefore select(testKey1)
      row1.columnsValues should contain key TestCol1
      row1.columnsValues(TestCol1).value shouldBe "valueRow1Col1"
      row1.columnsValues should contain key TestCol2
      row1.columnsValues(TestCol2).value shouldBe "valueRow1Col2"
      rowsBefore should containRowWithKey(testKey2)
      val row2 = rowsBefore select(testKey2)
      row2.columnsValues should contain key TestCol1
      row2.columnsValues(TestCol1).value shouldBe "valueRow2Col1"
      row2.columnsValues should contain key TestCol2
      row2.columnsValues(TestCol2).value shouldBe "valueRow2Col2"

      withTable(testTable) { implicit table =>
        delete(testKey1, TestCol1)
        delete(testKey2, List(TestCol1, TestCol2))
      }
      val rowsAfter1 = withTable(testTable) { implicit table =>
        scan(List(TestCol1, TestCol2))
      }
      rowsAfter1 should containRowWithKey(testKey1)
      val row1after = rowsAfter1 select testKey1
      row1after.columnsValues should not contain key(TestCol1)
      row1after.columnsValues should contain key(TestCol2)
      row1after.columnsValues(TestCol2).value shouldBe "valueRow1Col2"
      rowsAfter1 shouldNot containRowWithKey(testKey2)

      withTable(testTable) { implicit table =>
        delete(testKey1, TestCol2)
      }
      val rowsAfter2 = withTable(testTable) { implicit table =>
        scan(List(TestCol1, TestCol2))
      }
      rowsAfter2 shouldNot containRowWithKey(testKey1)
    }
  }

  it should "delete all rows for a given column family" in {
    def testKey(name: String) = TestRowKey(LocalDate.of(2017, 4, 4), name)

    case class TestCF1DynColumn(override val name: String) extends Column with TestCF1
    val testCol1Dynamic = TestCF1DynColumn("test_col1")
    val testCol2Dynamic = TestCF1DynColumn("test_col2")
    val columnFactory: PartialFunction[String, TestCF1DynColumn] =  {
      case (name: String) => TestCF1DynColumn(name)
    }

    val testKey1 = testKey("ABC")
    val testKey2 = testKey("DEF")

    withConnection { implicit conn =>
      withTable(testTable) { implicit table =>
        put(
          testKey1,
          Map(
            TestCol1 -> Some("ABCCol1value"),
            TestCol2 -> Some("ABCCol2value")
          )
        )
        put(
          testKey2,
          Map(
            TestCol1 -> Some("DEFCol1value"),
            TestCol2 -> Some("DEFCol2value")
          )
        )
      }
      val rowsBefore = withTable(testTable) { implicit table =>
        scan[TestRowKey, TestCF1, TestCF1DynColumn](TestCF1, columnFactory)
      }

      rowsBefore should containRowWithKey(testKey1)
      val row1 = rowsBefore select testKey1
      row1.columnsValues(testCol1Dynamic).value shouldBe "ABCCol1value"
      row1.columnsValues(testCol2Dynamic).value shouldBe "ABCCol2value"
      rowsBefore should containRowWithKey(testKey2)
      val row2 = rowsBefore select testKey2
      row2.columnsValues(testCol1Dynamic).value shouldBe "DEFCol1value"
      row2.columnsValues(testCol2Dynamic).value shouldBe "DEFCol2value"

      withTable(testTable) { implicit table =>
        delete(testKey2, TestCF1)
      }
      val rowsAfter1 = withTable(testTable) { implicit table =>
        scan[TestRowKey, TestCF1, TestCF1DynColumn](TestCF1, columnFactory)
      }
      rowsAfter1 should containRowWithKey(testKey1)
      val row1after = rowsBefore select testKey1
      row1after.columnsValues(testCol1Dynamic).value shouldBe "ABCCol1value"
      row1after.columnsValues(testCol2Dynamic).value shouldBe "ABCCol2value"
      rowsAfter1 shouldNot containRowWithKey(testKey2)

      withTable(testTable) { implicit table =>
        delete(testKey1, TestCF1)
      }
      val rowsAfter2 = withTable(testTable) { implicit table =>
        scan[TestRowKey, TestCF1, TestCF1DynColumn](TestCF1, columnFactory)
      }
      rowsAfter2 shouldNot containRowWithKey(testKey1)
    }
  }

  it should "remove all rows when truncating table" in {
    def testKey(name: String) = TestRowKey(LocalDate.of(2017, 6, 6), name)

    val testKey1 = testKey("testTruncate1")
    val testKey2 = testKey("testTruncate2")
    withConnection { implicit conn =>
      withTable(testTable) { implicit table =>
        put(
          testKey1,
          Map(
            TestCol1 -> Some("someCol1value")
          )
        )
        put(
          testKey2,
          Map(
            TestCol1 -> Some("someOtherCol1value")
          )
        )
      }

      val rowPrefix = TestRowKeyPrefix(LocalDate.of(2017, 6, 6))
      val rowsBefore = withTable(testTable) { implicit table =>
        scan(rowPrefix, TestCol1)
      }

      rowsBefore should have size 2

      truncate(testTable)

      val rowsAfter = withTable(testTable) { implicit table =>
        scan(rowPrefix, TestCol1)
      }

      rowsAfter should have size 0
    }
  }

  it should "put rows and columns at different moments in time, and retrieve the values of multiple rows at a particular moment in time" in {
    val testKey1 = TestRowKey(LocalDate.of(2018, 4, 19), "Row1")
    val testKey2 = TestRowKey(LocalDate.of(2018, 4, 19), "Row2")
    val testKey3 = TestRowKey(LocalDate.of(2018, 4, 20), "Row3")

    implicit def long2Instant(ts: Long): Instant = Instant.ofEpochMilli(ts)

    withConnection { implicit conn =>
      withTable(testTable) { implicit table =>
        put(testKey1, Map(TestCol1 -> Some("oldValueRow1Col1")), Some(10L))
        put(testKey1, Map(TestCol2 -> Some("oldValueRow1Col2")), Some(10L))
        put(testKey1, Map(TestCol1 -> Some("middleValueRow1Col1")), Some(14L))
        put(testKey1, Map(TestCol2 -> Some("middleValueRow1Col2")), Some(14L))
        put(testKey1, Map(TestCol1 -> Some("newValueRow1Col1")), Some(30L))
        put(testKey1, Map(TestCol2 -> Some("newValueRow1Col2")), Some(30L))

        put(testKey2, Map(TestCol1 -> Some("oldValueRow2Col1")), Some(10L))
        put(testKey2, Map(TestCol2 -> Some("oldValueRow2Col2")), Some(10L))
        put(testKey2, Map(TestCol1 -> Some("middleValueRow2Col1")), Some(18L))
        put(testKey2, Map(TestCol2 -> Some("middleValueRow2Col2")), Some(18L))
        put(testKey2, Map(TestCol1 -> Some("newValueRow2Col1")), Some(30L))
        put(testKey2, Map(TestCol2 -> Some("newValueRow2Col2")), Some(30L))

        put(testKey3, Map(TestCol1 -> Some("oldValueRow3Col1")), Some(10L))
        put(testKey3, Map(TestCol2 -> Some("oldValueRow3Col2")), Some(14L))
        put(testKey3, Map(TestCol1 -> Some("newValueRow3Col1")), Some(21L))
        put(testKey3, Map(TestCol2 -> Some("newValueRow3Col2")), Some(25L))
      }

      val rowPrefix = TestRowKeyPrefix(LocalDate.of(2018, 4, 19))
      val snapshotTime = 18L
      val rows = withTable(testTable) { implicit table =>
        scan(rowPrefix, List(TestCol1, TestCol2), snapshotTime)
      }

      rows should containRowWithKey(testKey1)
      val row1 = rows select testKey1
      row1.columnsValues(TestCol1).value shouldBe "middleValueRow1Col1"
      row1.columnsValues(TestCol2).value shouldBe "middleValueRow1Col2"
      rows should containRowWithKey(testKey2)
      val row2 = rows select testKey2
      row2.columnsValues(TestCol1).value shouldBe "middleValueRow2Col1"
      row2.columnsValues(TestCol2).value shouldBe "middleValueRow2Col2"
      rows shouldNot containRowWithKey(testKey3)
    }
  }

  it should "keep up to three versions for a column when configured as such" in {
    withConnection { implicit conn =>
      val testKey = TestRowKey(LocalDate.of(2018, 1, 16), "someRow")
      for (i <- 1 to 4)
        withTable(testTable) { implicit table =>
          put(testKey, Map(TestCol1 -> Some("value" + i)))
        }

      val values = withTable(testTable) { implicit table =>
        getVersionsForColumn(testKey, TestCol1)
      }

      val valueSet = values.value
      valueSet should have size 3
      valueSet.toList.map(_.value) shouldBe List("value4", "value3", "value2")
    }
  }

  it should "put rows and columns at different moments in time, and retrieve the values of one row at a particular moment in time" in {
    withConnection { implicit conn =>
      val testKey = TestRowKey(LocalDate.of(2018, 4, 19), "rijtje")

      for (i <- 1 to 4) {
        val updateTimestamp = Some(Instant.ofEpochMilli(i))

        withTable(testTable) { implicit table =>
          put(
            testKey,
            Map(TestCol1 -> Some("col1value" + i), TestCol2 -> Some("col2value" + i)),
            updateTimestamp
          )
        }
      }
      val snapshotTime = Instant.ofEpochMilli(3L)
      val columns = List(TestCol1, TestCol2)

      val snapshotResult = withTable(testTable) { implicit table =>
        get(testKey, columns, snapshotTime)
      }

      snapshotResult.value.columnsValues(TestCol1).value shouldBe "col1value3"
      snapshotResult.value.columnsValues(TestCol2).value shouldBe "col2value3"
    }
  }

}
