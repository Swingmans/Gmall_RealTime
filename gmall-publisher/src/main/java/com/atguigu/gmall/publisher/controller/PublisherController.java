package com.atguigu.gmall.publisher.controller;


import com.atguigu.gmall.publisher.bean.ByHourText;
import com.atguigu.gmall.publisher.service.PublisherService;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@RestController
public class PublisherController {

    @Autowired
    private PublisherService publisherService;

    @GetMapping("realtime-total")
    public List<Map<String,Object>> getTotal(String date) {
        Long dauCount = publisherService.getDauTotalByDate(date);
        List<Map<String,Object>> list = new LinkedList<>();
        Map<String,Object> map1 = new HashMap<String,Object>();
        map1.put("id","dau");
        map1.put("name","新增日活");
        map1.put("value",dauCount);
        list.add(map1);
        Map<String,Object> map2 = new HashMap<String,Object>();
        map2.put("id","new_mid");
        map2.put("name","新增设备");
        map2.put("value",355);
        list.add(map2);
        Double totalAmountByDate = publisherService.getTotalAmountByDate(date);
        Map<String,Object> map3 = new HashMap<String,Object>();
        map3.put("id","order_amount");
        map3.put("name","新增交易额");
        map3.put("value",totalAmountByDate);
        list.add(map3);
        return list;
    }


    @GetMapping("realtime-hour")
    public ByHourText getAmountHoursByDate(String id, String date) {
        if (!StringUtils.equals(id,"order_amount")){
            return new ByHourText();
        }
        return publisherService.getAmountHoursByDate(date);
    }
}
