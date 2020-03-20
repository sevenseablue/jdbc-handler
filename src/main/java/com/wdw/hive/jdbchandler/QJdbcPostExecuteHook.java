package com.wdw.hive.jdbchandler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.log4j.Logger;

import java.util.Arrays;

public class QJdbcPostExecuteHook implements ExecuteWithHookContext {
  private static final Logger log = Logger.getLogger(QJdbcPostExecuteHook.class);

  public void run(HookContext hookContext) throws Exception {
    Configuration sessionConf = hookContext.getConf();
    String peKey = "hive.exec.post.hooks";
    String peVal = sessionConf.get(peKey);
    String peValUp = peVal.replaceAll(QJdbcPostExecuteHook.class.getName() + ",", "");
    sessionConf.set(peKey, peValUp);

    String localAutoKey = "hive.exec.mode.local.auto";
    String localAutoVal = SessionState.get().getHiveVariables().get("hive.exec.mode.local.auto.prejdbc");
    sessionConf.set(localAutoKey, localAutoVal);
    log.info("####QJdbcPostExecuteHook####" + "\t" + peKey + "\t" + peVal + "\t" + peValUp + "\t" + localAutoVal);
  }

  public static void main(String[] args) {
    Arrays.stream(new String[]{"datadev", "test", "aa"}).forEach(x -> {
        }
    );
  }

}