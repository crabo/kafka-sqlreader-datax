package com.nascent.pipeline.datax.mysql;

import com.alibaba.datax.common.spi.ErrorCode;

public enum GenericErrorCode implements ErrorCode {
	    //连接错误
	    ERROR("error-01","errors occured");

	private final String code;

    private final String description;

    private GenericErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s]. ", this.code,
                this.description);
    }
}
