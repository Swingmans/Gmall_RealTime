package com.atguigu.gmall.logger

import org.springframework.boot.SpringApplication

object LoggerApplication {

  def main(args: Array[String]): Unit = {
     SpringApplication.run(classOf[LoggerApplicationConfig])
  }
}

