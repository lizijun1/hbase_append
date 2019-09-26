package com.lizijun.test

import java.text.SimpleDateFormat
import java.util.{Date, UUID}

object TestTime {
  def main(args: Array[String]): Unit = {
    val date: Date = new Date()

    val simple: SimpleDateFormat = new SimpleDateFormat("YYYYMMddHHmm")

    val format1: String = simple.format(date)

    println(format1)
    val substring: String = UUID.randomUUID().toString.substring(0,6)

    println(substring)

  }
}
