package com.bytedance.bitsail.connector.legacy.tableau.sink;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.column.Column;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.connector.legacy.tableau.common.TableauManager;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.connector.legacy.tableau.constants.WriteModeProxy;
import com.bytedance.bitsail.connector.legacy.tableau.constants.WriteModeProxy.WriteMode;
import com.bytedance.bitsail.connector.legacy.tableau.exception.TableauPluginErrorCode;
import com.bytedance.bitsail.connector.legacy.tableau.options.TableauWriterOptions;
import com.bytedance.bitsail.flink.core.legacy.connector.OutputFormatPlugin;
import com.google.common.collect.Maps;
import lombok.AccessLevel;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;


public class TableauOutputFormat extends OutputFormatPlugin<Row> {
    /**
     * 使用指定类初始化日志对象，在日志输出的时候，可以打印出日志信息所在类
     */
    private static final Logger logger = LoggerFactory.getLogger(TableauOutputFormat.class);
    private static final DateTimeFormatter DEFAULT_DATE_TIME_PATTERN_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final long serialVersionUID = 1L;
    private static final int MAX_PARALLELISM_OUTPUT_TABLEAU = 3;

    /**
     * write record numbers of each batch, better not trigger binary split
     */
    private int batchInterval;
    private String department;
    private String datastoreName;
    private String region;
    private String partitionPatternFormat;

    private Map<String, Object> partitionFilter;
    private boolean enableRecordsValidation;

    private int recordsValidationTimeoutSeconds;

    private transient DateTimeFormatter partitionDateFormat;

    private LocalDateTime partition;
    private List<ColumnInfo> columns;

    private WriteModeProxy writeModeProxy;
    private WriteMode writeMode;

    enum WriteTableType {
        // corresponding to fact datastore
        fact,
        // corresponding to dimension datastore(which can only use overwrite write mode)
        dimension
    }

    private WriteTableType writeTableType;

    /**
     * 当为动态分区时， key为动态分区值；当指定分区时，key为指定分区值
     */
    private Map<LocalDateTime, Queue<Map<String, Object>>> partitionRecordMap = new HashMap<>();

    /**
     * 在client端和task manager端各生成一份
     */
    @Setter(AccessLevel.PACKAGE)
    private transient TableauManager tableauManager;

    private long finalSuccessRecordCount;


    @Override
    public void initPlugin() throws Exception {
        department = outputSliceConfig.getNecessaryOption(TableauWriterOptions.DEPARTMENT, TableauPluginErrorCode.REQUIRED_VALUE);
        datastoreName = outputSliceConfig.getNecessaryOption(TableauWriterOptions.DATASTORE_NAME, TableauPluginErrorCode.REQUIRED_VALUE);
        region = outputSliceConfig.getNecessaryOption(TableauWriterOptions.REGION, TableauPluginErrorCode.REQUIRED_VALUE);
        partitionPatternFormat = outputSliceConfig.getNecessaryOption(TableauWriterOptions.PARTITION_PATTERN_FORMAT, TableauPluginErrorCode.REQUIRED_VALUE);
        partitionDateFormat = DateTimeFormatter.ofPattern(partitionPatternFormat);
        columns = outputSliceConfig.getNecessaryOption(TableauWriterOptions.COLUMNS, TableauPluginErrorCode.REQUIRED_VALUE);
        partitionFilter = outputSliceConfig.getUnNecessaryOption(TableauWriterOptions.PARTITION_FILTER, null);
        enableRecordsValidation = outputSliceConfig.getUnNecessaryOption(TableauWriterOptions.ENABLE_RECORDS_VALIDATION, true);
        recordsValidationTimeoutSeconds = outputSliceConfig.getUnNecessaryOption(TableauWriterOptions.RECORDS_VALIDATION_TIMEOUT_SECONDS, 3600);
        batchInterval = outputSliceConfig.getUnNecessaryOption(TableauWriterOptions.WRITE_BATCH_INTERVAL, 10000);
        logger.info("tableau init plugin finished, datastore_name: {}, " +
                        "department: {}, region: {}, batch_interval: {}, partition_pattern_format: {}",
                datastoreName, department, region, batchInterval, partitionPatternFormat);
        writeMode = WriteMode.valueOf(outputSliceConfig.getUnNecessaryOption(TableauWriterOptions.WRITE_MODE, WriteMode.insert.name()));
        writeModeProxy = buildWriteModeProxy(writeMode);
        if (writeTableType != WriteTableType.fact && writeTableType != WriteTableType.dimension) {
            throw BitSailException.asBitSailException(TableauPluginErrorCode.INTERNAL_ERROR, "Unsupported write table type: " + writeTableType);
        }
        writeTableType = WriteTableType.valueOf(outputSliceConfig.getUnNecessaryOption(TableauWriterOptions.WRITE_TABLE_TYPE, WriteTableType.fact.name()));
        tableauManager = new TableauManager(department, datastoreName, null, region);
        writeModeProxy.prepareOnClient();

    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        super.open(taskNumber, numTasks);
        tableauManager = new TableauManager(department, datastoreName, null, region);
        partitionDateFormat = DateTimeFormatter.ofPattern(partitionPatternFormat);
        writeModeProxy.prepareOnTM();
    }


    @Override
    public void writeRecordInternal(Row row) throws Exception {
        Map<String, Object> recordMap = Maps.newHashMap();
        LocalDateTime recordPartition = partition;
        for (int index = 0; index < row.getArity(); index++) {
            String columnName = columns.get(index).getName();
            Column column = (Column) row.getField(index);
            if (null == column.getRawData()) {
                recordMap.put(columnName, null);
                continue;
            }
            String columnType = StringUtils.lowerCase(columns.get(index).getType());
            switch (columnType) {
                case "bigint":
                case "int":
                    recordMap.put(columnName, column.asLong());
                    break;
                case "varchar":
                case "string":
                    recordMap.put(columnName, column.asString());
                    break;
                case "decimal":
                    recordMap.put(columnName, column.asDouble());
                    break;
                default:
                    throw  BitSailException
                        .asBitSailException(
                            CommonErrorCode.UNSUPPORTED_COLUMN_TYPE,
                            String.format(
                                "The column data type in your configuration is not support. Column index:[%d], Column type:[%s]." +
                                        " Please try to change the column data type or don't transmit this column.",
                                index,
                                columnType
                            )
                        );
            }

            Queue<Map<String, Object>> partitionRecordQueue = partitionRecordMap.get(recordPartition);
            partitionRecordQueue.offer(recordMap);
            if (partitionRecordQueue.size() >= batchInterval) {
                // execute batch
                flush(recordPartition);
            }
        }

    }

    private void flush(LocalDateTime recordPartition) {
        List<Map<String, Object>> recordList = new ArrayList<>();
        Queue<Map<String, Object>> partitionRecordQueue = partitionRecordMap.get(recordPartition);
        Map<String, Object> recordMap;

        while ((recordMap = partitionRecordQueue.poll()) != null) {
            recordList.add(recordMap);
        }

        if (writeTableType == WriteTableType.fact) {
            tableauManager.addRecords(recordList, recordPartition);
        } else {
            tableauManager.updateDimention(recordList);
        }
    }

    @Override
    public String getType() {
        return "Tableau";
    }

    @Override
    public int getMaxParallelism() {
        return MAX_PARALLELISM_OUTPUT_TABLEAU;
    }

    @Override
    public void close() throws IOException {
        flush(partition);
        super.close();
    }

    @Override
    public void tryCleanupOnError() throws Exception {

    }

    public class InsertProxy implements WriteModeProxy {
        @Override
        public void prepareOnTM() {
        }

        @Override
        public void prepareOnClient() throws IOException {
            String dataTimeString = outputSliceConfig.getNecessaryOption(TableauWriterOptions.PARTITION_VALUE, TableauPluginErrorCode.REQUIRED_VALUE);
            partition = LocalDate.parse(dataTimeString, partitionDateFormat).atTime(0, 0, 0);
            partitionRecordMap.put(partition, new LinkedBlockingQueue<>());

            if (writeTableType == WriteTableType.fact) {
                tableauManager.clearRecords(partition, partitionFilter);
                tableauManager.openRecord(partition);
            }

        }

        @Override
        public void onFailureComplete() throws IOException {
            WriteModeProxy.super.onFailureComplete();
        }

        @Override
        public void afterWriteRecord(int index) {
            WriteModeProxy.super.afterWriteRecord(index);
        }
    }

    public class OverwriteProxy implements WriteModeProxy {
        @Override
        public void prepareOnTM() {
            WriteModeProxy.super.prepareOnTM();
        }

        @Override
        public void prepareOnClient() throws IOException {
            String dateTimeString = outputSliceConfig.getNecessaryOption(TableauWriterOptions.PARTITION_VALUE, TableauPluginErrorCode.REQUIRED_VALUE);
            partition = LocalDate.parse(dateTimeString, partitionDateFormat).atTime(0, 0, 0);
            partitionRecordMap.put(partition, new LinkedBlockingQueue<>());
        }

        @Override
        public void onFailureComplete() throws IOException {
            if (writeTableType == WriteTableType.fact) {
                tableauManager.completeRecord(partition, partitionFilter);
            }
        }

        @Override
        public void afterWriteRecord(int index) {
            if (writeTableType == WriteTableType.fact) {
                tableauManager.completeRecord(partition, partitionFilter);
            }
        }
    }

    private WriteModeProxy buildWriteModeProxy(WriteMode writeMode) {
        switch (writeMode) {
            case insert:
                return new InsertProxy();
            case overwrite:
                return new OverwriteProxy();
            default:
                throw BitSailException.asBitSailException(CommonErrorCode.CONFIG_ERROR, "unsupported write mode: " + writeMode);
        }
    }
}