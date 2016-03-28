package com.zhwb.canal.client.parser;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.zhwb.canal.client.domain.CanalEvent;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author zhangwenbin
 * @since 2016/1/19.
 */
public abstract class CanalParser {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(CanalParser.class);

    public static List<CanalEntry.EventType> validEventTypes = new ArrayList<CanalEntry.EventType>() {{
        add(CanalEntry.EventType.INSERT);
        add(CanalEntry.EventType.UPDATE);
        add(CanalEntry.EventType.DELETE);
    }};


    public static List<CanalEvent> parse(List<CanalEntry.Entry> entries) {
        List<CanalEvent> result = Lists.newArrayList();

        CanalEntry.RowChange rowChange;
        for (CanalEntry.Entry entry : entries) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }
            try {
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());

                CanalEntry.EventType eventType = rowChange.getEventType();
                if (!validEventTypes.contains(eventType)) {
                    continue;
                }

                for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                    CanalEvent event = new CanalEvent(entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
                            eventType.name(), parseColumnList(rowData.getBeforeColumnsList()), parseColumnList(rowData.getAfterColumnsList()));
                    result.add(event);
                }
            } catch (Exception e) {
                LOGGER.error("ERROR ## parser of eromanga-event has an error , data:{}, exception:{}", entry.toString(), e.getMessage());
            }
        }
        return result;
    }

    private static Map<String, String> parseColumnList(List<CanalEntry.Column> columnsList) {
        if (CollectionUtils.isEmpty(columnsList)) {
            return null;
        }
        Map<String, String> result = Maps.newHashMap();
        for (CanalEntry.Column column : columnsList) {
            result.put(column.getName(), column.getValue());
        }
        return result;
    }

}
