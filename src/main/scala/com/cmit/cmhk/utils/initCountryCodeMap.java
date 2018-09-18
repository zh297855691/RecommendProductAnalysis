package com.cmit.cmhk.utils;

import java.util.concurrent.ConcurrentHashMap;

public class initCountryCodeMap {

    private static ConcurrentHashMap<String,String> map = new ConcurrentHashMap<>();
    private static initCountryCodeMap initMap = null;

    public ConcurrentHashMap<String, String> getMap() {
        return map;
    }

    public void setMap(ConcurrentHashMap<String, String> map) {
        initCountryCodeMap.map = map;
    }

    public static initCountryCodeMap getInitMap() {
        if (initMap == null) {
            initMap = new initCountryCodeMap();
        }
        return initMap;
    }

}
