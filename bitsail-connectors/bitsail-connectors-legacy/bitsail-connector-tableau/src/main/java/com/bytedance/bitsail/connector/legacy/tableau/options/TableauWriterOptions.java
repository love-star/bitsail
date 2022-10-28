package com.bytedance.bitsail.connector.legacy.tableau.options;

import com.bytedance.bitsail.common.annotation.Essential;
import com.bytedance.bitsail.common.option.ConfigOption;
import com.bytedance.bitsail.common.option.WriterOptions;

import java.util.Map;

import static com.bytedance.bitsail.common.option.ConfigOptions.key;
import static com.bytedance.bitsail.common.option.WriterOptions.WRITER_PREFIX;

public interface TableauWriterOptions extends WriterOptions.BaseWriterOptions {

    ConfigOption<String> WRITER_CLASS =
            key(WRITER_PREFIX + "class")
                    .defaultValue("com.bytedance.bitsail.connector.legacy.tableau.TableauOutputFormat");

    @Essential
    ConfigOption<String> DEPARTMENT =
            key(WRITER_PREFIX + "department")
                    .noDefaultValue(String.class);
    @Essential
    ConfigOption<String> DATASTORE_NAME =
            key(WRITER_PREFIX + "datastore_name")
                    .noDefaultValue(String.class);
    @Essential
    ConfigOption<String> REGION =
            key(WRITER_PREFIX + "region")
                    .noDefaultValue(String.class);

    ConfigOption<String> PARTITION_NAME =
            key(WRITER_PREFIX + "partition_name")
                    .noDefaultValue(String.class);

    ConfigOption<String> PARTITION_VALUE =
            key(WRITER_PREFIX + "partition_value")
                    .noDefaultValue(String.class);

    @Essential
    ConfigOption<String> PARTITION_PATTERN_FORMAT =
            key(WRITER_PREFIX + "partition_pattern_format")
                    .noDefaultValue(String.class);

    ConfigOption<Integer> WRITE_BATCH_INTERVAL =
            key(WRITER_PREFIX + "write_batch_interval")
                    .noDefaultValue(Integer.class);

    ConfigOption<String> WRITE_MODE =
            key(WRITER_PREFIX + "write_mode")
                    .noDefaultValue(String.class);

    ConfigOption<String> WRITE_TABLE_TYPE =
            key(WRITER_PREFIX + "write_table_type")
                    .noDefaultValue(String.class);

    /**
     * 在tableau写入完毕时校验写入的条数是否与dalaran返回条数一致
     */
    ConfigOption<Boolean> ENABLE_RECORDS_VALIDATION =
            key(WRITER_PREFIX + "enable_records_validation")
                    .defaultValue(true);

    /**
     * record校验超时时间,超时后抛出异常
     */
    ConfigOption<Integer> RECORDS_VALIDATION_TIMEOUT_SECONDS =
            key(WRITER_PREFIX + "records_validation_timeout_seconds")
                    .defaultValue(3600);

    /**
     * 支持配置tableau 分区过滤条件, 并按照过滤条件删除数据
     * https://athens.bytedance.net/sites/project/73/interfaces/345/3024
     * 例如{"extras": {"key1": "value1", "key2": "value2", "key3": "value3"}}
     */
    ConfigOption<Map<String, Object>> PARTITION_FILTER =
            (ConfigOption) key(WRITER_PREFIX + "partition_filter")
                    .noDefaultValue(Map.class);
}
