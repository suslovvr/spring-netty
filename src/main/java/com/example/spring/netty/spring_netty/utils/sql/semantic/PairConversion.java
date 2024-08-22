package com.example.spring.netty.spring_netty.utils.sql.semantic;

public record PairConversion<K, V>(K semanticName, V realName) {
    public static <K, V> PairConversion<K, V> of(K semanticName, V realName) {
        return new PairConversion<>(semanticName, realName);
    }
}