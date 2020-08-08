package timeusage

import org.apache.spark.sql.{ColumnName, DataFrame, Row}
import org.apache.spark.sql.types.{
  DoubleType,
  StringType,
  StructField,
  StructType
}
import org.junit.runner.RunWith
import org.junit.{Assert, Test}
import org.junit.Assert.assertEquals
//import org.scalatest.junit.JUnitRunner
//import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.util.Random

//@RunWith(classOf[JUnitRunner])
//class TimeUsageSuite extends FunSuite with BeforeAndAfterAll {

class TimeUsageSuite {
  @Test
  def sample1 = {
    Samples.sample1()
    val pass = try {
      Samples.sample1()
      true
    } catch { case _: Throwable => false }
    assert(pass, "Didn't pass")
  }

  @Test
  def sample2 = {
    val pass = try {
      Samples.sample2()
      true
    } catch { case _: Throwable => false }
    assert(pass, "Didn't pass")
  }

  @Test
  def sample3 = {
    val pass = try { Samples.sample3(); true } catch { case _: Throwable => false }
    assert(pass, "Didn't pass")
  }

/*
  @Test
  def stop = {
    val pass = try { Samples.stop(); true } catch { case _: Throwable => false }
    assert(pass, "Didn't stop")
  }
*/
}
