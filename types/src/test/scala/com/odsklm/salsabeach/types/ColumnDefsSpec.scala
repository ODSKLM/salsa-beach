package com.odsklm.salsabeach.types

import com.odsklm.salsabeach.types.ColumnDefs._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ColumnDefsSpec extends AnyFlatSpec with Matchers {

  trait SomeColumnFamily extends ColumnFamily { val columnFamily = "column_family" }
  trait SomeColumn extends Column with SomeColumnFamily

  case object LoadSequenceNumber extends SomeColumn
  case object StowageDeviceIdentifier extends SomeColumn { override val name = "something_else" }
  case object HeadStationDepartureDateUTC extends SomeColumn
  case object TimeDiffInboundATALastGateChange extends SomeColumn

  trait OutboundPaxDepartureLookupColumn extends Column with SomeColumnFamily
  case object EstimatedBlockDepartureCTime extends SomeColumn

  case class OutboundPaxDeparture(override val name: String)
      extends OutboundPaxDepartureLookupColumn {}

  it should "properly create HBase column names" in {
    LoadSequenceNumber.columnFamily shouldBe "column_family"

    LoadSequenceNumber.name shouldBe "load_sequence_number"

    StowageDeviceIdentifier.name shouldBe "something_else"

    HeadStationDepartureDateUTC.name shouldBe "head_station_departure_date_utc"

    EstimatedBlockDepartureCTime.name shouldBe "estimated_block_departure_c_time"

    TimeDiffInboundATALastGateChange.name shouldBe "time_diff_inbound_ata_last_gate_change"

    new OutboundPaxDeparture("abracadabra_pilatus_pas").name shouldBe "abracadabra_pilatus_pas"
  }
}
