package com.atguigu.gmall.canal.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.atguigu.gmall.common.constant.GmallConstant;
import org.apache.commons.lang.StringUtils;

import java.util.List;

public class CanalHandle {

    private String tableName;

    private List<CanalEntry.RowData> rowDatasList;

    private CanalEntry.EventType eventType;

    public CanalHandle(String tableName, List<CanalEntry.RowData> rowDatasList, CanalEntry.EventType eventType) {
        this.tableName = tableName;
        this.rowDatasList = rowDatasList;
        this.eventType = eventType;
    }

    public void handle() {
        if (StringUtils.equals(tableName,"order_info") && eventType == CanalEntry.EventType.INSERT) {
            //TODO 推送kafka到对应的分区
            pushMessageToKafka("GMALL_ORDER");
            return;
        }
        if (StringUtils.equals(tableName,"user_info") && eventType == CanalEntry.EventType.INSERT) {
            //TODO 推送kafka到对应的分区
            pushMessageToKafka("GMALL_USER");
        }
    }

    private void pushMessageToKafka(String topic) {
        for (CanalEntry.RowData rowData : rowDatasList) {
            //每一行数据 --> json
            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                jsonObject.put(column.getName(), column.getValue());
            }
            String jsonStr = jsonObject.toJSONString();
            MyKafkaUtil.sendMessage(topic,jsonStr);
        }
    }
}
