package com.lizijun

import java.sql.{DriverManager, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.{sql, util}
import java.util.{Date, UUID}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{Cell, CellScanner, HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter._


object TestHbase {
  def main(args: Array[String]): Unit = {
    //创建conf
    val conf: Configuration = HBaseConfiguration.create()
    //加载配置文件
    conf.addResource("../hbase-site.xml")
    //创建连接
    val conn: Connection = ConnectionFactory.createConnection(conf)
    //列族
    val strings: Array[String] = Array("lisi", "zhangsan")
    //添加数据
   /* insertTable(conn, "exam", "lisi", "aa", "5")
    insertTable(conn, "exam", "lisi", "aa", "5")
    insertTable(conn, "exam", "lisi", "aa", "5")
    insertTable(conn, "exam", "lisi", "aa", "5")
    insertTable(conn, "exam", "lisi", "aa", "5")
    insertTable(conn, "exam", "lisi", "aa", "5")
    insertTable(conn, "exam", "lisi", "aa", "5")*/
    //查询，过滤
    scanDataFilter(conn, "exam")
  }


  def scanDataFilter(connection: Connection, table: String): Unit = {
    //获取表
    val table1: Table = connection.getTable(TableName.valueOf(table))
    //创建实参对象
    val scan: Scan = new Scan()
    //查询出来时间戳
    val rowKey: String = select()
    //过滤时间戳
    val rowFilter: RowFilter = new RowFilter(CompareOp.GREATER_OR_EQUAL, new SubstringComparator(rowKey))
    //设置过滤条件
    scan.setFilter(rowFilter)
    //获取数据
    val scanner: ResultScanner = table1.getScanner(scan)
    //进行遍历
    val iterator: util.Iterator[Result] = scanner.iterator()
    while (iterator.hasNext) {
      val next: Result = iterator.next()
      //转成array
      val x: Array[Cell] = next.rawCells()
      //输出
      x.map(x => {
        println(
            new String(x.getRowArray, x.getRowOffset, x.getRowLength) + " " +
            new String(x.getFamilyArray, x.getFamilyOffset, x.getFamilyLength) + " " +
            new String(x.getQualifierArray, x.getQualifierOffset, x.getQualifierLength) + " " +
            new String(x.getValueArray, x.getValueOffset, x.getValueLength))
      })
    }

  }

  /**
    *
    * @param time 向mysql里添加增量的开始时间
    */
  def add(time: String): Unit = {
    //加载驱动
    Class.forName("com.mysql.jdbc.Driver")
    //连接mysql
    val connection: sql.Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/1701c", "root", "root")
    //添加时间
    val prepareStatement: PreparedStatement = connection.prepareStatement("insert into  hbase(time) values (" + time + ")")
    //执行
    prepareStatement.execute()

  }

  /**
    *
    * @return 查询出来最大的id的时间戳
    */
  def select(): String = {
    //变量
    var i = ""
    Class.forName("com.mysql.jdbc.Driver")
    val connection: sql.Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/1701c", "root", "root")

    val ps: PreparedStatement = connection.prepareStatement("select time from hbase where id = (select max(id) from hbase) ")
    val rs: ResultSet = ps.executeQuery()
    while (rs.next()) {
      i = rs.getString(1)
    }
    i
  }

  /**
    * 向hbase里添加数据
    * @param connection
    * @param table
    * @param columnFamily
    * @param column
    * @param value
    */
  def insertTable(connection: Connection, table: String, columnFamily: String, column: String, value: String): Unit = {
    //获取时间
    val date: Date = new Date()
    //时间格式
    val simple: SimpleDateFormat = new SimpleDateFormat("YYYYMMddHHmm")
    //时间到分钟
    val format1: String = simple.format(date)
    //随机数
    val random: String = UUID.randomUUID().toString.substring(0, 6)
    //拼接成rowkey
    val rowkey: String = random + format1
    //调用方法向mysql里添加时间
    add(format1)
    //向hbase里添加数据
    val put: Put = new Put(rowkey.getBytes())

    put.addColumn(columnFamily.getBytes(), column.getBytes(), value.getBytes())

    val table1: Table = connection.getTable(TableName.valueOf(table))

    table1.put(put)

    table1.close()
  }
}
