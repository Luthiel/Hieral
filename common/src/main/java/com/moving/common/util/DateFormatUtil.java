package com.moving.common.util;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class DateFormatUtil {
    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter dtfForPartition = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final DateTimeFormatter dtfFull = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * 将时刻时间转换为时间戳
     * @param dateTime
     * @return
     */
    public static Long dateTimeToTs(String dateTime) {
        LocalDateTime localDateTime = LocalDateTime.parse(dateTime, dtfFull);
        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }

    /**
     * 将时间戳转换为日期
     * @param ts
     * @return
     */
    public static String tsToDate(Long ts) {
        Date dt = new Date(ts);
        LocalDateTime localDateTime =
                LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
        return dtf.format(localDateTime);
    }

    /**
     * 将时间戳转换为时刻时间
     * @param ts
     * @return
     */
    public static String tsToDateTime(Long ts) {
        Date dt = new Date(ts);
        LocalDateTime localDateTime =
                LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
        return dtfFull.format(localDateTime);
    }

    /**
     * 将时间戳转换为 短横线和冒号间隔 格式的时刻时间
     * @param ts
     * @return
     */
    public static String tsToDateForPartition(long ts) {
        Date dt = new Date(ts);
        LocalDateTime localDateTime =
                LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
        return dtfForPartition.format(localDateTime);
    }

    /**
     * 将日期时间转换为时间戳
     * @param date
     * @return
     */
    public static long dateToTs(String date) {
        return dateTimeToTs(date + " 00:00:00");
    }

}
