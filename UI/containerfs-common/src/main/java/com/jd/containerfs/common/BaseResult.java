package com.jd.containerfs.common;

/**
 * Created by lixiaoping3 on 17-11-17.
 */
public class BaseResult<T> {

    /**
     * 成功标识
     */
    private boolean isSuccess;
    /**
     * 状态码
     */
    private Integer resultCode;
    /**
     * 描述
     */
    private String resultMessage;

    private T obj;

    public boolean isSuccess() {
        return isSuccess;
    }

    public void setSuccess(boolean isSuccess) {
        this.isSuccess = isSuccess;
    }

    public Integer getResultCode() {
        return resultCode;
    }

    public void setResultCode(Integer resultCode) {
        this.resultCode = resultCode;
    }

    public String getResultMessage() {
        return resultMessage;
    }

    public void setResultMessage(String resultMessage) {
        this.resultMessage = resultMessage;
    }

    public T getObj() {
        return obj;
    }

    public void setObj(T obj) {
        this.obj = obj;
    }

}
