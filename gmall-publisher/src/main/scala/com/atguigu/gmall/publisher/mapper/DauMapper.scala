package com.atguigu.gmall.publisher.mapper

trait DauMapper {

  /**
   * 根据日期查询当前的日活总数
   * @param date
   * @return
   */
  def getDauTotalByDate(date: String): Long



}
