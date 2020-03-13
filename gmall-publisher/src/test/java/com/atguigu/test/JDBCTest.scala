package com.atguigu.test

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}


object JDBCTest {

  def main(args: Array[String]): Unit = {
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    val connection: Connection = DriverManager.getConnection("jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181")
    val statement: PreparedStatement = connection.prepareStatement("select count(1) from gmall2019_dau where logDate = '2020-03-13'")
    val set: ResultSet = statement.executeQuery()
    println(set.getObject(1))
  }

}
