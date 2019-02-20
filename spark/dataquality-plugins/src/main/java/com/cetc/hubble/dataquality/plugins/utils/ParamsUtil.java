package com.cetc.hubble.dataquality.plugins.utils;

import org.stringtemplate.v4.ST;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParamsUtil {
    private static String QUOTE  = "'";
    public static Map<FixedParamEnum, String> parseFixed(String[] args) {
        Map map = new HashMap();
        map.put(FixedParamEnum.INPUT_TYPE, args[0]);
        map.put(FixedParamEnum.INPUT_IP, args[1]);
        map.put(FixedParamEnum.INPUT_PORT, args[2]);
        map.put(FixedParamEnum.INPUT_DB_NAME, args[3]);
        map.put(FixedParamEnum.INPUT_TABLE, args[4]);
        map.put(FixedParamEnum.INPUT_USERNAME, args[5]);
        map.put(FixedParamEnum.INPUT_PASSWORD, args[6]);
        map.put(FixedParamEnum.OUT_URL, args[7]);
        map.put(FixedParamEnum.OUT_TABLE, args[8]);
        map.put(FixedParamEnum.OUT_USERNAME, args[9]);
        map.put(FixedParamEnum.OUT_PASSWORD, args[10]);
        return map;
    }
    public static Map<FixedParamEnum, String> parseFixedJX(String[] args) {
        Map map = new HashMap();
        map.put(FixedParamEnum.INPUT_TYPE, args[0]);
        map.put(FixedParamEnum.INPUT_IP, args[1]);
        map.put(FixedParamEnum.INPUT_PORT, args[2]);
        map.put(FixedParamEnum.INPUT_DB_NAME, args[3]);
        map.put(FixedParamEnum.INPUT_TABLE, args[4]);
        map.put(FixedParamEnum.INPUT_USERNAME, args[5]);
        map.put(FixedParamEnum.INPUT_PASSWORD, args[6]);
        return map;
    }
    public static String mkString(String[] arr, String sep){
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < arr.length; i++){
            if (arr.length == i+1){
                sb.append(arr[i]);
                break;
            }
            sb.append(arr[i]).append(sep);
        }
        return sb.toString();
    }

    public static String eraseQuote(String arg) {
        Pattern pattern = Pattern.compile("^'(.*)'$");
        Matcher matcher = pattern.matcher(arg);
        if(matcher.find()){
            arg = matcher.group(1);
        }
        return  arg;
    }
}
