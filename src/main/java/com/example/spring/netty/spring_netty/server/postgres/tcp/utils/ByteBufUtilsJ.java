package com.example.spring.netty.spring_netty.server.postgres.tcp.utils;

import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class ByteBufUtilsJ {

    public static Map<String, String> readCStringMap(final ByteBuf msg) {
        final Map<String, String> map = new java.util.HashMap<>();
        while (msg.readableBytes() > 0) {
            final String key = readCString(msg);
            if (key == null) {
                break;
            }
            final String value = readCString(msg);
            map.put(key, value);
        }
        return map;
    }

    public static String readCString(final ByteBuf msg) {
        final byte[] bytes = new byte[msg.bytesBefore((byte) 0) + 1];
        if (bytes.length == 0) {
            return null;
        }
        msg.readBytes(ByteBuffer.wrap(bytes));
        final String result = new String(bytes, 0, bytes.length - 1, StandardCharsets.UTF_8);
        return result;
    }
}
