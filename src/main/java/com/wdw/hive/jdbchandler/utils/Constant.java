package com.wdw.hive.jdbchandler.utils;

/**
 * Created with Lee. Date: 2019/8/30 Time: 10:25 To change this template use File | Settings | File
 * Templates. Description:
 *
 * @author :
 */

public class Constant {

  public static final String MYSQL_PXC_PREFIX = "hive.sql.pxc";
  public static final String MYSQL_QMHA_PREFIX = "hive.sql.qmha";
//  public static final String OVERWRITE_WHITE_USERNAME = "hive.insert.overwrite.white.username";

  public static int DEFAULT_FETCH_SIZE;
  public static Integer DEFAULT_SPLIT_SIZE;
  public static int BATCH_SIZE;
  public static String MYSQL_USERNAME = "data_hive_rw";
  public static String MYSQL_PASSWORD = "data_hive_rw_ps";
  public static String MYSQL_USERNAME_R = "data_hive_r";
  public static String MYSQL_PASSWORD_R = "data_hive_r_ps";
  public static String MYSQL_USERNAME_W = "data_hive_rw";
  public static String MYSQL_PASSWORD_W = "data_hive_rw_ps";
  public static String POSTGREP_USERNAME = "hadoop_analyse_beta";
  public static String POSTGREP_PASSWORD = "6159eb69-e2ef-4dca-a3ce-bc7fc70b2b84";
  public static String POSTGREP_USERNAME_R = "hadoop_analyse_beta";
  public static String POSTGREP_PASSWORD_R = "6159eb69-e2ef-4dca-a3ce-bc7fc70b2b84";
  public static String POSTGREP_USERNAME_W = "hadoop_analyse_beta";
  public static String POSTGREP_PASSWORD_W = "6159eb69-e2ef-4dca-a3ce-bc7fc70b2b84";
  public static final String STAR6 = "******";
  static {
    try {
      Constant.class.getClassLoader().loadClass("com.wdw.hive.jdbchandler.utils.PropUtil");
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    PropUtil.initConstant();
  }

}
