package com.wdw.hive.jdbchandler.authority;

import com.wdw.hive.jdbchandler.exception.HiveJdbcAuthorityException;
import com.wdw.hive.jdbchandler.utils.LogUtil;
import com.wdw.hive.jdbchandler.utils.PropUtil;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import javax.sql.DataSource;
import org.apache.commons.dbcp.BasicDataSourceFactory;

/**
 * Created with Lee. Date: 2019/9/10 Time: 11:34 To change this template use File | Settings | File
 * Templates. Description:
 *
 * @author :
 */

public class AuthJdbc {

  private final static LogUtil LOGGER = LogUtil.getLogger();
  protected DataSource dataSource = null;

  protected void initializeDatabaseConnection() throws Exception {
    if (dataSource == null) {
      synchronized (this) {
        if (dataSource == null) {
          Properties props = getConnectionPoolProperties();
          dataSource = BasicDataSourceFactory.createDataSource(props);
        }
      }
    }
  }

  protected static Connection getConnection() throws HiveJdbcAuthorityException {
    AuthJdbc authorityDao = new AuthJdbc();
    try {
      authorityDao.initializeDatabaseConnection();
      return authorityDao.dataSource.getConnection();
    } catch (Exception e) {
      LOGGER.error("init conn faile", e);
      throw new HiveJdbcAuthorityException("init conn faile");
    }
  }

  protected Properties getConnectionPoolProperties() {
    Properties dbProperties = null;
    dbProperties = PropUtil.getAuthorityProp();
    dbProperties.put("type", "javax.sql.DataSource");
    dbProperties.put("initialSize", "1");
    dbProperties.put("maxActive", "1");
    dbProperties.put("maxIdle", "1");
    dbProperties.put("maxWait", "10000");
    dbProperties.put("timeBetweenEvictionRunsMillis", "30000");
    return dbProperties;
  }

  protected static void cleanupResources(Connection conn, PreparedStatement ps, ResultSet rs) {
    try {
      if (rs != null) {
        rs.close();
      }
    } catch (SQLException e) {
      LOGGER.warn("Caught exception during resultset cleanup.", e);
    }

    try {
      if (ps != null) {
        ps.close();
      }
    } catch (SQLException e) {
      LOGGER.warn("Caught exception during statement cleanup.", e);
    }

    try {
      if (conn != null) {
        conn.close();
      }
    } catch (SQLException e) {
      LOGGER.warn("Caught exception during connection cleanup.", e);
    }
  }

}
