package timeusage

import org.apache.spark.sql.{ColumnName, DataFrame, Row}
import org.apache.spark.sql.types.{
  DoubleType,
  StringType,
  StructField,
  StructType
}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with BeforeAndAfterAll {

  test("sample1") {
    val pass = try {
      Samples.sample1()
      true
    } catch { case _: Throwable => false }
    assert(pass, "Didn't pass")
  }

  test("sample2") {
    val pass = try {
      Samples.sample2()
      true
    } catch { case _: Throwable => false }
    assert(pass, "Didn't pass")
  }

  test("sample3") {
    val pass = try { Samples.sample3(); true } catch { case _: Throwable => false }
    assert(pass, "Didn't pass")
  }

  test("stop") {
    val pass = try { Samples.stop(); true } catch { case _: Throwable => false }
    assert(pass, "Didn't stop")
  }
}
