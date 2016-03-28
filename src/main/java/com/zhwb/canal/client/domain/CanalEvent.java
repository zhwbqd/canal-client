package com.zhwb.canal.client.domain;

import com.google.common.base.Objects;

import java.io.Serializable;
import java.util.Map;

/**
 * @author zhangwenbin
 */
public class CanalEvent implements Serializable {

    private static final long serialVersionUID = 119L;

    private String schemaName;
    private String tableName;
    private String eventType;
    private Map<String, String> rowBefore;
    private Map<String, String> rowAfter;

    public CanalEvent(String schemaName, String tableName, String eventType, Map<String, String> rowBefore, Map<String, String> rowAfter) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.eventType = eventType;
        this.rowBefore = rowBefore;
        this.rowAfter = rowAfter;
    }

    public CanalEvent() {
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public Map<String, String> getRowBefore() {
        return rowBefore;
    }

    public void setRowBefore(Map<String, String> rowBefore) {
        this.rowBefore = rowBefore;
    }

    public Map<String, String> getRowAfter() {
        return rowAfter;
    }

    public void setRowAfter(Map<String, String> rowAfter) {
        this.rowAfter = rowAfter;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("schemaName", schemaName)
                .add("tableName", tableName)
                .add("eventType", eventType)
                .add("rowBefore", rowBefore)
                .add("rowAfter", rowAfter)
                .toString();
    }
}
