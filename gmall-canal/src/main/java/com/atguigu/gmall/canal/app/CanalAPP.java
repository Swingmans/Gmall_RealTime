package com.atguigu.gmall.canal.app;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;


public class CanalAPP {

    public static void main(String[] args) {
        //创建canal连接
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("hadoop102", 11111),
                "example",
                "",
                ""
        );

        canalConnector.connect();
        canalConnector.subscribe("gmall0513.*");

        //不断的循环拉取批次数据
        while (true) {
            //一批次拉取100个sql的操作结果集
            Message message = canalConnector.get(100);
            List<CanalEntry.Entry> entries = message.getEntries();
            if (entries.size() == 0) {
                System.out.println("没有数据,稍微休息5s");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                for (CanalEntry.Entry entry : entries) {
                    //判断每个sql的操作结果集是否是ROWDATA
                    if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
                        CanalEntry.RowChange rowChange = null;
                        try {
                            rowChange = CanalEntry
                                            .RowChange
                                                .parseFrom(entry.getStoreValue());
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                        //获取操作结果集
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        CanalHandle canalHandle = new CanalHandle(
                                entry.getHeader().getTableName(),
                                rowDatasList,
                                rowChange.getEventType()
                        );
                        canalHandle.handle();
                    }
                }
            }
        }
    }
}
