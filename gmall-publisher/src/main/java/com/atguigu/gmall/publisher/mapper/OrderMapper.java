package com.atguigu.gmall.publisher.mapper;

import com.atguigu.gmall.publisher.bean.HourAmount;

import java.util.List;

public interface OrderMapper {

    public Double getTotalAccount(String date);

    public List<HourAmount> getTotalHoursByDate(String date);
}
