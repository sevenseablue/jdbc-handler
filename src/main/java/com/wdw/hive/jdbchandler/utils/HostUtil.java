package com.wdw.hive.jdbchandler.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created with Lee. Date: 2019/9/10 Time: 15:56 To change this template use File | Settings | File
 * Templates. Description:
 *
 * @author :
 */

public class HostUtil {

  public static String getIp(String host) {
    String ip = null;
    try {
      ip = InetAddress.getAllByName(host)[0].getHostAddress();
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
    return ip;
  }

  public static boolean isProd(){
    boolean online = false;
    try {
      if (InetAddress.getLocalHost().getHostName().replaceAll(".wdw.com$", "").matches("l-.*\\.cn[1-9a]")) {
        online = true;
      }
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
    return online;
  }

  public static boolean isLocal(){
    boolean online = true;
    try {
      if (InetAddress.getLocalHost().getHostName().replaceAll(".wdw.com$", "").matches("l-.*\\.cn[0-9a]")) {
        online = false;
      }
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
    return online;
  }

  public static String getHost(String ip) {
    String host = null;
    try {
      host = InetAddress.getAllByName(ip)[0].getHostName();
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
    return host;
  }
}
