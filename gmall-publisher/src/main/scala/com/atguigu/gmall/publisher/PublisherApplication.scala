package com.atguigu.gmall.publisher

import org.mybatis.spring.annotation.MapperScan
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication

@SpringBootApplication
@MapperScan(basePackages = Array[String]("com.atguigu.gmall.publisher.mapper"))
class PublisherApplication {

}

object PublisherApplication {
  def main(args: Array[String]): Unit = {
    SpringApplication.run(classOf[PublisherApplication])
  }
}