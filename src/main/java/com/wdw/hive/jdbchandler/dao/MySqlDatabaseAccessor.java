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
package com.wdw.hive.jdbchandler.dao;

import com.wdw.hive.jdbchandler.conf.JdbcStorageConfigManager;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

/**
 * MySQL specific data accessor. This is needed because MySQL JDBC drivers do not support generic
 * LIMIT and OFFSET escape functions
 */
public class MySqlDatabaseAccessor extends GenericJdbcDatabaseAccessor {

  @Override
  protected String addLimitAndOffsetToQuery(String sql, int limit, int offset) {
    if (offset == 0) {
      return addLimitToQuery(sql, limit);
    } else {
      return sql + " LIMIT " + offset + "," + limit;
    }
  }


  @Override
  protected String addLimitToQuery(String sql, int limit) {
    if (limit == -1) {
      return sql;
    }
    return sql + " LIMIT " + limit;
  }

  @Override
  public String buildQuery(Configuration conf, String table, List<String> columnNames) {
    if (columnNames == null) {
      throw new IllegalArgumentException("Field names may not be null");
    }
    JdbcStorageConfigManager.removeIgnoreColumns(conf, columnNames);
    StringBuilder query = new StringBuilder();
//    String rp = JdbcStorageConfig.REPLACE.getConfigValue(conf);
//    if (rp != null && rp.equals("true")) {
//      query.append("REPLACE INTO ").append(table);
//    } else {
      query.append("INSERT INTO ").append(table);
//    }
    query.append(" (");
    query.append(StringUtils.join(columnNames, ","));
    query.append(")");
    query.append(" VALUES (");
    for (int i = 0; i < columnNames.size(); i++) {
      query.append("?,");
    }
    query.deleteCharAt(query.length() - 1);
    query.append(");");
    LOGGER.info("mysql insert sql is [{}]", query.toString());
    return query.toString();
  }
}
