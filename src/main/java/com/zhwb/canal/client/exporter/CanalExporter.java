package com.zhwb.canal.client.exporter;

import com.zhwb.canal.client.util.ConfigUtils;
import com.zhwb.canal.client.domain.CanalEvent;
import com.zhwb.canal.client.util.ConfigConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author zhangwenbin
 * @since 2016/1/19.
 */
public abstract class CanalExporter {
 private static final Logger LOGGER = LoggerFactory.getLogger(CanalExporter.class);
    public static CanalExporter getExporter(ConfigUtils config) {
        String exportType = config.getString("canal.export.type");
        if (exportType.equals(ConfigConstant.EXPORT_TYPE_AMQ)) {
            return AmqCanalExporter.getInstance(config);
        } else {
            throw new UnsupportedOperationException("Canal export type not supported, " + exportType);
        }
    }

    public abstract void exportReal(List<CanalEvent> canalEventList);

    public void export(List<CanalEvent> canalEventList){
        LOGGER.info("ready to export event, data: {}", canalEventList);
        exportReal(canalEventList);
    }
}
