package com.wdw.hive.jdbchandler.utils;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.Properties;

/**
 * Created with Lee. Date: 2019/9/24 Time: 16:53 To change this template use File | Settings | File
 * Templates. Description:
 *
 * @author :
 */

public class PropUtil {

  private static Properties systemProp = new Properties();
  private static boolean online = false;

  static {
    try {
      if (InetAddress.getLocalHost().getHostName().replaceAll(".wdw.com$", "").matches("l-.*\\.cn[1-9a]")) {
        online = true;
      }
      InputStream resourceAsStream = null;
      if(online) {
        resourceAsStream = PropUtil.class.getClassLoader().getResourceAsStream("jdbc-handler.properties");
      }else{
        resourceAsStream = PropUtil.class.getClassLoader().getResourceAsStream("jdbc-handler-beta.properties");
      }
      systemProp.load(resourceAsStream);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static String getProperty(String key) {
    return systemProp.getProperty(key);
  }

  public static String getProperty(String key, String defalut) {
    return systemProp.getProperty(key, defalut);
  }

  public static void initConstant() {
    Constant.DEFAULT_FETCH_SIZE = Integer.parseInt(PropUtil.getProperty("hive.default.fetch.size"));
    Constant.BATCH_SIZE = Integer.parseInt(PropUtil.getProperty("hive.default.batch.size"));
    Constant.DEFAULT_SPLIT_SIZE = Integer.parseInt(PropUtil.getProperty("hive.default.split.size"));
//    if (online) {
      Constant.MYSQL_USERNAME = PropUtil.getProperty("hive.mysql.username");
      Constant.MYSQL_PASSWORD = PropUtil.getProperty("hive.mysql.password");
      Constant.MYSQL_USERNAME_R = PropUtil.getProperty("hive.mysql.username.r");
      Constant.MYSQL_PASSWORD_R = PropUtil.getProperty("hive.mysql.password.r");
      Constant.MYSQL_USERNAME_W = PropUtil.getProperty("hive.mysql.username.w");
      Constant.MYSQL_PASSWORD_W = PropUtil.getProperty("hive.mysql.password.w");

      Constant.POSTGREP_USERNAME = PropUtil.getProperty("hive.postgres.username");
      Constant.POSTGREP_PASSWORD = PropUtil.getProperty("hive.postgres.password");
      Constant.POSTGREP_USERNAME_R = PropUtil.getProperty("hive.postgres.username.r");
      Constant.POSTGREP_PASSWORD_R = PropUtil.getProperty("hive.postgres.password.r");
      Constant.POSTGREP_USERNAME_W = PropUtil.getProperty("hive.postgres.username.w");
      Constant.POSTGREP_PASSWORD_W = PropUtil.getProperty("hive.postgres.password.w");
//    }
  }

  public static Properties getAuthorityProp() {
    Properties prop = new Properties();
    if (online) {
      try (InputStream resourceAsStream = PropUtil.class.getClassLoader()
          .getResourceAsStream("authority.properties")) {
        prop.load(resourceAsStream);
      } catch (IOException e) {
        e.printStackTrace();
      }
    } else {
      try (InputStream resourceAsStream = PropUtil.class.getClassLoader()
              .getResourceAsStream("authority-beta.properties")) {
        prop.load(resourceAsStream);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    return prop;
  }

  public static void main(String[] args) {
    System.out.println(Constant.MYSQL_PASSWORD_R);
  }
}
