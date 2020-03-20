package com.wdw.hive.jdbchandler;

import java.sql.Date;
import java.sql.Time;

public class Test {
    public static void main(String[] args) {
        System.out.println(Date.valueOf("2020-01-01"));
        System.out.println(Time.valueOf("01:10:00"));
        System.out.println(QJdbcPostExecuteHook.class.getName());
    }
}
