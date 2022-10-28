package com.bytedance.bitsail.connector.legacy.tableau.exception;

import com.bytedance.bitsail.common.exception.ErrorCode;

public enum TableauPluginErrorCode implements ErrorCode {
    REQUIRED_VALUE("TableauPlugin-01", "You missed parameter which is required, please check your configuration."),
    ILLEGAL_VALUE("TableauPlugin-02", "The parameter value in your configuration is not valid."),
    INTERNAL_ERROR("TableauPlugin-03", "Internal error occurred, which is usually a BitSail bug."),
    EXTERNAL_ERROR("TableauPlugin-04", "External error occurred, which is usually a tableau bug.");
    private final String code;
    private final String description;
    private TableauPluginErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s].", this.code,
                this.description);
    }
}
