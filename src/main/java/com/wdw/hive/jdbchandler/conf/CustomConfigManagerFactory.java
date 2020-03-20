/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wdw.hive.jdbchandler.conf;

import java.util.Properties;
import org.apache.commons.lang.StringUtils;

/**
 * Factory for creating custom config managers based on the database type
 */
public class CustomConfigManagerFactory {

  private static CustomConfigManager nopConfigManager = new NopCustomConfigManager();


  private CustomConfigManagerFactory() {
  }


  public static CustomConfigManager getCustomConfigManagerFor(DatabaseType databaseType) {
    switch (databaseType) {
      case MYSQLQMHA:
        return new QmhaConfigManager();
      case MYSQLPXC:
        return new PxcConfigManager();
      default:
        return nopConfigManager;
    }
  }

  private static class NopCustomConfigManager implements CustomConfigManager {

    /**
     * enum JdbcStorageConfig.isRequired() DATABASE_TYPE("database.type", true), JDBC_URL("url",
     * true), JDBC_DRIVER_CLASS("driver", true), USERNAME("username", true), PASSWORD("password",
     * true),
     */
    @Override
    public void checkRequiredProperties(Properties props) {
      for (JdbcStorageConfig jdbcStorageConfig : JdbcStorageConfig.values()) {
        if (jdbcStorageConfig.isRequired()) {
          String value = props.getProperty(jdbcStorageConfig.getPropertyName());
          if (StringUtils.isBlank(value)) {
            throw new IllegalArgumentException(
                String.format("%s is required,please check", jdbcStorageConfig.getPropertyName()));
          }
        }
      }
    }

  }


  private static class PxcConfigManager implements CustomConfigManager {

    /**
     * namespace, username, password, dbname, can not be emtpy
     */
    @Override
    public void checkRequiredProperties(Properties props) {
      String namespace = props.getProperty("hive.sql.pxc.namespace");
      String username = props.getProperty(JdbcStorageConfig.USERNAME.getPropertyName());
      String password = props.getProperty(JdbcStorageConfig.PASSWORD.getPropertyName());
      String dbname = props.getProperty("hive.sql.pxc.dbname");
      if (StringUtils.isBlank(namespace) || StringUtils.isBlank(username) || StringUtils
          .isBlank(password) || dbname == null) {
        throw new IllegalArgumentException(String
            .format("pxc param is not required [namespcase:%s,username:%s,password:%s,dbname:%s]",
                namespace, username, password, dbname));
      }
    }

  }

  private static class QmhaConfigManager implements CustomConfigManager {

    /**
     * namespace, username, password, dbname, can not be emtpy
     */
    @Override
    public void checkRequiredProperties(Properties props) {
      String namespace = props.getProperty("hive.sql.qmha.namespace");
      String username = props.getProperty(JdbcStorageConfig.USERNAME.getPropertyName());
      String password = props.getProperty(JdbcStorageConfig.PASSWORD.getPropertyName());
      String dbname = props.getProperty("hive.sql.qmha.dbname");
      if (StringUtils.isBlank(namespace) || StringUtils.isBlank(username) || StringUtils
          .isBlank(password) || StringUtils.isBlank(dbname)) {
        throw new IllegalArgumentException(String
            .format("qmha param is not required [namespcase:%s,username:%s,password:%s,dbname:%s]",
                namespace, username, password, dbname));
      }
    }

  }

}
