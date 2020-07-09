package com.odsklm.salsabeach

import java.time.Instant
import java.time.temporal.ChronoUnit

import com.odsklm.salsabeach.types.ColumnDefs.{Column, ColumnFamily}
import com.odsklm.salsabeach.types.models.Value
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.{CellUtil, KeyValue}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.SortedSet

class PackageSpec extends AnyFlatSpec with Matchers {

  trait CF extends ColumnFamily { override val columnFamily = "CF" }
  object Col1 extends Column with CF
  object Col2 extends Column with CF

  "convertCellsWithColumnsToRow" should "convert a HBase Result to a salsa-beach Row" in {
    val rowKey = "row1"
    val cf = "CF"
    val tNow = Instant.now.truncatedTo(ChronoUnit.MILLIS)
    val tMinus5s = tNow.minusSeconds(5)
    val tMinus10s = tNow.minusSeconds(10)

    val c1Col1 = CellUtil.createCell(rowKey, cf, Col1.name, tMinus5s.toEpochMilli, KeyValue.Type.Put.getCode, "col1val1")
    val c2Col1 = CellUtil.createCell(rowKey, cf, Col1.name, tNow.toEpochMilli, KeyValue.Type.Put.getCode, "col1val2")
    val c3Col1 = CellUtil.createCell(rowKey, cf, Col1.name, tMinus10s.toEpochMilli, KeyValue.Type.Put.getCode, "col1val3")

    val c4Col2 = CellUtil.createCell(rowKey, cf, Col2.name, tNow.toEpochMilli, KeyValue.Type.Put.getCode, "col2val4")
    val c5Col2 = CellUtil.createCell(rowKey, cf, Col2.name, tMinus5s.toEpochMilli, KeyValue.Type.Put.getCode, "col2val5")

    val hBaseresult = Result.create(Array(c1Col1, c2Col1, c3Col1, c4Col2, c5Col2))

    val v1 = Value("col1val1", tMinus5s)
    val v2 = Value("col1val2", tNow)
    val v3 = Value("col1val3", tMinus10s)

    val v4 = Value("col2val4", tNow)
    val v5 = Value("col2val5", tMinus5s)

    val col1Values = SortedSet(v2, v1, v3)
    val col2Values = SortedSet(v4, v5)

    val result = convertCellsToColumnsWithVersionedValues(hBaseresult, List(Col1, Col2))
    result(Col1).toSet shouldEqual col1Values.toSet
    result(Col2).toSet shouldEqual col2Values.toSet
  }

}
