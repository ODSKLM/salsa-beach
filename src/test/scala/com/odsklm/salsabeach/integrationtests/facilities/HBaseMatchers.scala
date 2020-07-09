package com.odsklm.salsabeach.integrationtests.facilities

import com.odsklm.salsabeach.types.models.{Row, Value}
import org.scalatest.matchers.{MatchResult, Matcher}

trait HBaseMatchers {

  class OptionalValueMatcher(expectedValue: String) extends Matcher[Option[Value]] {

    def apply(left: Option[Value]): MatchResult = {
      MatchResult(
        left.isDefined && left.get.value == expectedValue,
        s"""Value $left did not match "$expectedValue"""",
        s"""Value $left matched "$expectedValue""""
      )
    }
  }

  def matchOptionalValue(expectedValue: String) = new OptionalValueMatcher(expectedValue)

  class RowMatcher[A](expectedRowKey: A) extends Matcher[List[Row[A]]] {
    def apply(left: List[Row[A]]): MatchResult = {
      MatchResult(
        left.exists(_.rowKey == expectedRowKey),
        s"""Row with row key $expectedRowKey does not exist in $left""",
        s"""Row with row key $expectedRowKey exists in $left"""
      )
    }
  }

  def containRowWithKey[A](expectedRowKey: A) = new RowMatcher[A](expectedRowKey)
}
