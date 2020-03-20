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

import com.wdw.hive.jdbchandler.utils.Constant;
import com.wdw.hive.jdbchandler.utils.LogUtil;
import org.apache.hadoop.conf.Configuration;

public enum JdbcStorageConfig {

  DATABASE_TYPE("database.type", true),
  JDBC_URL("url", true),
  JDBC_DRIVER_CLASS("driver", true),
  USERNAME("username", true),
  PASSWORD("password", true),
  QUERY("query", false),
  TABLE("table", false),
  PARTITION_COLUMN("partition.column", false),
  PARTITION_NUMS("partition.nums", false),
  JDBC_FETCH_SIZE("fetch.size", false),
  JDBC_BATCH_SIZE("batch.size", false),
  AUTO_SPLIT_SIZE("split.size", false),
//  REPLACE("replace", false),
  IGNORE_COLUMN("write.ignore.column", false),
  READ_WRITE("read-write", true),
  COLUMN_MAPPING("column.mapping", false),
  DELETE_TYPE("delete.type", false),
  DELETE_BYCOLUMN("delete.by.columns", false),
  PART_TABLES("part.tables", false),
  USERACCOUNT("useraccount", false);

  private String propertyName;
  private boolean required = false;
  private static final LogUtil LOGGER = LogUtil.getLogger();


  JdbcStorageConfig(String propertyName, boolean required) {
    this.propertyName = propertyName;
    this.required = required;
  }

  JdbcStorageConfig(String propertyName) {
    this.propertyName = propertyName;
  }

  public String getPropertyName() {
    return JdbcStorageConfigManager.CONFIG_PREFIX + "." + propertyName;
  }

  public String getConfigValue(Configuration conf) {
    return JdbcStorageConfigManager.getConfigValue(this, conf);
  }

  public String getConfigValue(Configuration conf, String defaultName) {
    return JdbcStorageConfigManager.getConfigValue(this, conf, defaultName);
  }

  public Integer getConfigIntValue(Configuration conf) {
    return JdbcStorageConfigManager.getConfigIntValue(this, conf);
  }

  public Integer getConfigIntValue(Configuration conf, Integer defaultValue) {
    return JdbcStorageConfigManager.getConfigIntValue(this, conf, defaultValue);
  }

  public String getUserName(Configuration conf) {
    DatabaseType databaseType = DatabaseType
        .valueOf(JdbcStorageConfig.DATABASE_TYPE.getConfigValue(conf));
    switch (databaseType) {
      case POSTGRES:
        if (JdbcStorageConfig.USERACCOUNT.getConfigValue(conf).equals("r")) {
          return Constant.POSTGREP_USERNAME_R;
        } else {
          return Constant.POSTGREP_USERNAME_W;
        }
      default:
        if (JdbcStorageConfig.USERACCOUNT.getConfigValue(conf).equals("r")) {
          return Constant.MYSQL_USERNAME_R;
        } else {
          return Constant.MYSQL_USERNAME_W;
        }
    }
  }

  public String getPassword(Configuration conf) {
    DatabaseType databaseType = DatabaseType
        .valueOf(JdbcStorageConfig.DATABASE_TYPE.getConfigValue(conf));
    switch (databaseType) {
      case POSTGRES:
        if (JdbcStorageConfig.USERACCOUNT.getConfigValue(conf).equals("r")) {
          return Constant.POSTGREP_PASSWORD_R;
        } else {
          return Constant.POSTGREP_PASSWORD_W;
        }
      default:
        if (JdbcStorageConfig.USERACCOUNT.getConfigValue(conf).equals("r")) {
          return Constant.MYSQL_PASSWORD_R;
        } else {
          return Constant.MYSQL_PASSWORD_W;
        }
    }
  }

  public boolean isRequired() {
    return required;
  }

}
