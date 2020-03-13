package com.atguigu.gmall.publisher.service;

import com.atguigu.gmall.publisher.bean.ByHourText;



public interface PublisherService {

    public Long getDauTotalByDate(String date);

    public Double getTotalAmountByDate(String date);

    public ByHourText getAmountHoursByDate(String date);
}
