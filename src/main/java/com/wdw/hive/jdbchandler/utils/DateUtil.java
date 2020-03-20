package com.wdw.hive.jdbchandler.utils;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {
  private static final LogUtil LOGGER = LogUtil.getLogger();

  public static Timestamp StrToTs(String strDate) {
    try {
      DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      Date date = formatter.parse(strDate);
      Timestamp timeStampDate = new Timestamp(date.getTime());
      return timeStampDate;
    } catch (ParseException e) {
      LOGGER.info("Timestamp parseException :" + e);
      return null;
    }
  }

  public static void main(String[] args) {
    System.out.println(StrToTs("2020-01-02 12:00:01"));
  }
}
