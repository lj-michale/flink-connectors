package com.aurora.flink.common;

/**
 * @author lj.michale
 * @description
 * @date 2021-07-08
 */
public class Util {
    public static void checkArgument(boolean condition, String message) {
        if(!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}