package com.trendyol.mpc.kafkathena.commons.model.constant;

public enum KSHeader {
    FILTER_KEY("filter_key");

    private String headerKeyName;

    KSHeader(String headerKeyName) {
        this.headerKeyName = headerKeyName;
    }

    public String getHeaderKeyName() {
        return this.headerKeyName;
    }
}
