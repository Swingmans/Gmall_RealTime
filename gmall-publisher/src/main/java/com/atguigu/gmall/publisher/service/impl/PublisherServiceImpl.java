package com.atguigu.gmall.publisher.service.impl;

import com.atguigu.gmall.publisher.bean.ByHourText;
import com.atguigu.gmall.publisher.bean.HourAmount;
import com.atguigu.gmall.publisher.mapper.DauMapper;
import com.atguigu.gmall.publisher.mapper.OrderMapper;


import com.atguigu.gmall.publisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@Service
public class PublisherServiceImpl implements PublisherService {
    @Autowired
    private DauMapper dauMapper;
    @Autowired
    private OrderMapper orderMapper;

    @Override
    public Long getDauTotalByDate(String date) {
        return dauMapper.getDauTotalByDate(date);
    }

    @Override
    public Double getTotalAmountByDate(String date) {
        return orderMapper.getTotalAccount(date);
    }

    @Override
    public ByHourText getAmountHoursByDate(String date) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date yesterday = null;
        try {
            yesterday = DateUtils.addDays(sdf.parse(date), -1);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        String yesterdayStr = sdf.format(yesterday);

        List<HourAmount> todayList = orderMapper.getTotalHoursByDate(date);
        List<HourAmount> yesterdayList = orderMapper.getTotalHoursByDate(yesterdayStr);

        return getTodayAndYesterdayHourText(todayList,yesterdayList);
    }

    private ByHourText getTodayAndYesterdayHourText(List<HourAmount> today, List<HourAmount> yesterday) {
        ByHourText byHourText = new ByHourText();

        Map<String,Double> map1 = new HashMap<>(32);
        for (HourAmount hourAmount : today) {
            String hour = hourAmount.getHour();
            Double amount = hourAmount.getAmount();
            map1.put(hour,amount);
        }
        byHourText.setToday(map1);

        Map<String,Double> map2 = new HashMap<>(32);
        for (HourAmount hourAmount : yesterday) {
            String hour = hourAmount.getHour();
            Double amount = hourAmount.getAmount();
            map1.put(hour,amount);
        }
        byHourText.setYesterday(map2);

        return byHourText;
    }
}
