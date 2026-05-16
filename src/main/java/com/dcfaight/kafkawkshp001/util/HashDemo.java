package com.dcfaight.kafkawkshp001.util;

import org.apache.kafka.common.utils.Utils;

public class HashDemo {
    public static void main(String[] args) {
        System.out.println(Utils.toPositive(Utils.murmur2("alice".getBytes()))); // e.g., 1379247716
        System.out.println(Utils.toPositive(Utils.murmur2("bob".getBytes())));   // e.g.,   89980845
        System.out.println(Utils.toPositive(Utils.murmur2("carol".getBytes()))); // e.g., 1992710649
    }
}
