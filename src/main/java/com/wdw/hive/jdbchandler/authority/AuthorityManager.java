package com.wdw.hive.jdbchandler.authority;

import com.wdw.hive.jdbchandler.authority.entry.AuthorityInfo;
import com.wdw.hive.jdbchandler.conf.DatabaseType;
import com.wdw.hive.jdbchandler.conf.JdbcStorageConfig;
import com.wdw.hive.jdbchandler.conf.JdbcStorageConfigManager;
import com.wdw.hive.jdbchandler.exception.HiveJdbcAuthorityException;
import com.wdw.hive.jdbchandler.utils.HostUtil;
import com.wdw.hive.jdbchandler.utils.LogUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;

/**
 * Created with Lee. Date: 2019/9/9 Time: 17:22 To change this template use File | Settings | File
 * Templates. Description:
 *
 * @author :
 */

public class AuthorityManager {

  private static final LogUtil LOGGER = LogUtil.getLogger();
  private static final Pattern PATTERM = Pattern.compile("jdbc:.*?:\\/\\/(.*?):(.*)?\\/(.*)");

  public static String getAuthority(Configuration conf) throws HiveJdbcAuthorityException {
    String tableName = JdbcStorageConfigManager.getTableName(conf);
    String username = JdbcStorageConfig.USERNAME.getConfigValue(conf);
    String password = JdbcStorageConfig.PASSWORD.getConfigValue(conf);
    String database = "";
    String namespace = "";
    AuthorityInfo authorityInfo = null;
    try {
      DatabaseType databaseType = DatabaseType
          .valueOf(JdbcStorageConfig.DATABASE_TYPE.getConfigValue(conf));
      switch (databaseType) {
        default:
          String ip = "";
          String port = "";
          String url = JdbcStorageConfig.JDBC_URL.getConfigValue(conf);
          Matcher matcher = PATTERM.matcher(url);
          if (matcher.find()) {
            ip = matcher.group(1);
            ip = HostUtil.getIp(ip);
            port = matcher.group(2);
            database = matcher.group(3);
          }
          LOGGER.info(String
              .format("pararm is : ip:%s,port:%s,database:%s,tablename:%s,username:%s,password:%s",
                  ip, port, database, tableName, username, password));
          authorityInfo = new AuthorityInfo(username, password, ip, port, database, tableName);
      }

      List<AuthorityInfo> authorityInfos = new ArrayList<>();
      authorityInfos.addAll(AuthorityDao.query(AuthJdbc.getConnection(), authorityInfo));
      authorityInfo.setTableName("*");
      authorityInfos.addAll(AuthorityDao.query(AuthJdbc.getConnection(), authorityInfo));
      return String.join(",", authorityInfos.stream().flatMap(x-> Arrays.stream(x.getPower().split(","))).collect(Collectors.toSet()));
    } catch (HiveJdbcAuthorityException e) {
      LOGGER.info("get authority failed:", e);
      throw new HiveJdbcAuthorityException(e.getMessage());
    }
  }

}
