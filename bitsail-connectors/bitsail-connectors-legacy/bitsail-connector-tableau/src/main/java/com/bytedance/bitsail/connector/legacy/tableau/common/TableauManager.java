package com.bytedance.bitsail.connector.legacy.tableau.common;

import com.bytedance.bitsail.common.util.HttpManager;
import com.bytedance.bitsail.common.util.HttpManager.WrappedResponse;
import com.bytedance.bitsail.common.util.Preconditions;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.collect.Maps;
import org.apache.http.HttpHost;
import org.apache.http.NoHttpResponseException;
import org.apache.http.entity.ContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TableauManager {
    private static Logger logger = LoggerFactory.getLogger(TableauManager.class);
    private static final DateTimeFormatter DEFAULT_DATE_TIME_PATTERN_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter DEFAULT_DATE_PATTERN_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private static final String xx_CONSUL = "dalaran.byted.org";

    private static final String STATUS = "status";
    private static final String EXTRAS = "extras";
    private static final int DEFAULT_TIMEOUT = 5 * 60;
    private static final int JSON_MAX_LENGTH = 64 * 1024;

    private static final Map<String, String> CONSUL_SERVER_MAP = Maps.newHashMap();

    static {
        CONSUL_SERVER_MAP.put("xxx", xx_CONSUL);
    }



    /**
     * 部门信息
     */
    private String department;

    /**
     * 表名称
     */
    private String dataStoreName;

    /**
     * 服务地址
     */
    private String server;

    /**
     * 所在集群
     */
    private String region;


    private String baseUrl;
    private int maxLength;
    private boolean isBigPackage;

    private Retryer<Integer> retryer;

    public TableauManager(String department, String dataStoreName, String server, String region) {
        this.department = department;
        this.dataStoreName = Preconditions.checkNotNull(dataStoreName, "The data store name is null or empty!");
        this.region = region;
        this.server = server == null || server.equals("") ? CONSUL_SERVER_MAP.getOrDefault(region, xx_CONSUL) : server;


        this.baseUrl = new HttpHost(this.server) + "/api/v1";

        retryer = RetryerBuilder.<Integer>newBuilder()
                //502返回码和NoHttpResponseException都重试
                .retryIfExceptionOfType(NoHttpResponseException.class)
                .retryIfResult(result -> result == 502)
                //重调策略
                .withWaitStrategy(WaitStrategies.exponentialWait(100, 5, TimeUnit.MINUTES))
                //尝试次数
                .withStopStrategy(StopStrategies.stopAfterAttempt(5))
                .build();
    }


    public WrappedResponse clearRecords(LocalDateTime dateTime, Map<String, Object> bodyConfig) throws IOException {
        dateTimeValueCheck(dateTime);
        String dateTimeStr = DEFAULT_DATE_TIME_PATTERN_FORMATTER.format(dateTime);
        String recordUrl = getRecordUrl(dateTimeStr);
        WrappedResponse response = HttpManager.sendDelete(recordUrl, null, bodyConfig, ContentType.APPLICATION_JSON);
        logger.info("clear tableau records finished, result: {}", response.getResult());
        return response;
    }

    private String getRecordUrl(String dateTimeStr) {
        return null;
    }

    private void dateTimeValueCheck(LocalDateTime dateTime) {
    }

    public void openRecord(LocalDateTime partition) {

    }

    public void addRecords(List<Map<String, Object>> records, LocalDateTime dateTime) {
        dateTimeValueCheck(dateTime);
        /**ß
         * 前期处理数据: 1. 格式化时间
         *             2. 切割数据大小
         */
        long systemTime = System.currentTimeMillis();
        String dateTimeStr = DEFAULT_DATE_TIME_PATTERN_FORMATTER.format(dateTime);

        // 记录每次发送的总条数
        logger.info("send tableau record, partition: {}, size: {}", dateTimeStr, records.size());
    }

    public void updateDimention(List<Map<String, Object>> recordList) {
    }

    public void completeRecord(LocalDateTime partition, Map<String, Object> partitionFilter) {

    }
}
