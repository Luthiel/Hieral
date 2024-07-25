package com.moving.utils;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * SimpleDateFormat 线程不安全，多线程环境下无法使用
 * JDK8 LocalDate LocalDatetime 是线程安全的
 */
public class DateUtil {
    public static final String PATTERN = "yyyy-MM-dd HH:mm:ss";
    public static LocalDateTime convertStr2Datetime(String event_time) {
        DateTimeFormatter dft = DateTimeFormatter.ofPattern(PATTERN);
        return LocalDateTime.parse(event_time, dft);
    }

    public static long convertLocalDateTime2Timestamp(LocalDateTime localDateTime) {
        // 时区校准后转换
        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }
}
