package com.atguigu.gmall.publisher.service.impl

import com.atguigu.gmall.publisher.mapper.DauMapper
import com.atguigu.gmall.publisher.service.PublisherService
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

@Service
class PublisherServiceImpl @Autowired()(val dauMapper:DauMapper) extends PublisherService{


  override def getDauCountByDate(date: String): Long = {
    dauMapper.getDauTotalByDate(date)
  }


}
