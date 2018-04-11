package com.rong360.model;

/**
 * @author zhangtao@rong360.com
 */
public class QueueData {

    private String message = "";
    private String routingKey = "";

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }


}
