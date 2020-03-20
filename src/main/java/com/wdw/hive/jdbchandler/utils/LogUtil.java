package com.wdw.hive.jdbchandler.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created with Lee. Date: 2019/8/29 Time: 19:47 To change this template use File | Settings | File
 * Templates. Description:
 *
 * @author :
 */

public class LogUtil {

  private static final Logger log = LoggerFactory.getLogger("JDBCHANDLER_LOG");
  private static final String LOGERCLASS = LogUtil.class.getName();
  private static LogUtil logger = new LogUtil();

  private LogUtil() {
  }

  public static LogUtil getLogger() {
    return logger;
  }


  private static String getPrefix() {
    StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
    StringBuilder prefixBuilder = new StringBuilder();
    for (StackTraceElement stackTraceElement : stackTrace) {
      String className = stackTraceElement.getClassName();
      if ("java.lang.Thread".equals(className) || LOGERCLASS.equals(className)) {
        continue;
      }
      prefixBuilder.append(className.substring(className.lastIndexOf(".") + 1) + ".");
      prefixBuilder.append(stackTraceElement.getMethodName());
      prefixBuilder.append("(" + stackTraceElement.getLineNumber() + ")######");
      break;
    }
    return prefixBuilder.toString();
  }

  public void trace(String arg0) {
    if (log.isTraceEnabled()) {
      log.trace(getPrefix() + arg0);
    }
  }

  public void trace(String arg0, Object arg1) {
    if (log.isTraceEnabled()) {
      log.trace(getPrefix() + arg0, arg1);
    }
  }

  public void trace(String arg0, Object... arg1) {
    if (log.isTraceEnabled()) {
      log.trace(getPrefix() + arg0, arg1);
    }
  }

  public void trace(String arg0, Throwable arg1) {
    if (log.isTraceEnabled()) {
      log.trace(getPrefix() + arg0, arg1);
    }
  }

  public void debug(String arg0) {
    if (log.isDebugEnabled()) {
      log.debug(getPrefix() + arg0);
    }
  }


  public void debug(String arg0, Object arg1) {
    if (log.isDebugEnabled()) {
      log.debug(getPrefix() + arg0, arg1);
    }
  }


  public void debug(String arg0, Object... arg1) {
    if (log.isDebugEnabled()) {
      log.debug(getPrefix() + arg0, arg1);
    }
  }


  public void debug(String arg0, Throwable arg1) {
    if (log.isDebugEnabled()) {
      log.debug(getPrefix() + arg0, arg1);
    }
  }

  public void info(Object arg0) {
    if (log.isInfoEnabled()) {
      log.info(getPrefix() + arg0);
    }
  }

  public void info(String arg0, Object arg1) {
    if (log.isInfoEnabled()) {
      log.info(getPrefix() + arg0, arg1);
    }
  }


  public void info(String arg0, Throwable arg1) {
    if (log.isInfoEnabled()) {
      log.info(getPrefix() + arg0, arg1);
    }
  }

  public void info(String arg0, Object... args) {
    if (log.isInfoEnabled()) {
      log.info(getPrefix() + arg0, args);
    }
  }

  public void warn(String arg0) {
    if (log.isWarnEnabled()) {
      log.warn(getPrefix() + arg0);
    }
  }

  public void warn(String arg0, Object arg1) {
    if (log.isWarnEnabled()) {
      log.warn(getPrefix() + arg0, arg1);
    }
  }

  public void warn(String arg0, Object... arg1) {
    if (log.isWarnEnabled()) {
      log.warn(getPrefix() + arg0, arg1);
    }
  }

  public void warn(String arg0, Throwable arg1) {
    if (log.isWarnEnabled()) {
      log.warn(getPrefix() + arg0, arg1);
    }
  }


  public void error(String arg0) {
    if (log.isErrorEnabled()) {
      log.error(getPrefix() + arg0);
    }
  }

  public void error(String arg0, Object arg1) {
    if (log.isErrorEnabled()) {
      log.error(getPrefix() + arg0, arg1);
    }
  }

  public void error(String arg0, Object... arg1) {
    if (log.isErrorEnabled()) {
      log.error(getPrefix() + arg0, arg1);
    }
  }


  public void error(String arg0, Throwable arg1) {
    if (log.isErrorEnabled()) {
      log.error(getPrefix() + arg0, arg1);
    }
  }


}
