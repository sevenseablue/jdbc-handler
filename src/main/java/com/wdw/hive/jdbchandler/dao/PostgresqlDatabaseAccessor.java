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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.wdw.hive.jdbchandler.exception.HiveJdbcDatabaseAccessException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

/**
 * MySQL specific data accessor. This is needed because MySQL JDBC drivers do not support generic
 * LIMIT and OFFSET escape functions
 */
public class PostgresqlDatabaseAccessor extends GenericJdbcDatabaseAccessor {

  @Override
  protected String addLimitAndOffsetToQuery(String sql, int limit, int offset) {
    if (offset == 0) {
      return addLimitToQuery(sql, limit);
    } else {
      return sql + " LIMIT " + limit + " OFFSET " + offset;
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
  public String buildQuery(Configuration conf, String table, List<String> columnNames)
      throws HiveJdbcDatabaseAccessException {
    String query = super.buildQuery(conf, table, columnNames);
    LOGGER.info("insert sql is [{}]", query);
//    String rp = JdbcStorageConfig.REPLACE.getConfigValue(conf);
//    if (StringUtils.isNotBlank(rp) && rp.trim().equals("true")) {
//      query = query.substring(0, query.length() - 1) + " " + buildPgConflictQuery(conf, table,
//          columnNames);
//      LOGGER.info("pg insert query add upsert : {} ", query);
//    }
    return query;
  }

  private String buildPgConflictQuery(Configuration conf, String table, List<String> columnNames)
      throws HiveJdbcDatabaseAccessException {
    StringBuilder query = new StringBuilder();

    Connection conn = null;
    try {
      initializeDatabaseConnection(conf);
      conn = dataSource.getConnection();
      ResultSet rs = conn.getMetaData().getIndexInfo(null, null, table, false, false);
      HashMap<String, Integer> indexMap = Maps.newHashMap();
      HashSet<String> cols = Sets.newHashSet();
      String index = "";
      String column = "";
      while (rs.next()) {
        index = rs.getString("INDEX_NAME");
        column = rs.getString("COLUMN_NAME");
        if (!columnNames.contains(column)) {
          continue;
        }
        if (indexMap.containsKey(index)) {
          indexMap.replace(index, indexMap.get(index) + 1);
        } else {
          indexMap.put(index, 1);
        }
        cols.add(column);
      }

      if (cols.size() == 0) {
        return query.toString();
      }

      if (cols.size() != Sets.newTreeSet(indexMap.values()).last()) {
        throw new HiveJdbcDatabaseAccessException(
            "muti uniue index need one large unique contain all columns");
      }

      query.append(String.format("ON CONFLICT(%s) do ", StringUtils.join(cols, ",")));
      Iterator<String> iterator = columnNames.iterator();
      query.append("update set ");
      while (iterator.hasNext()) {
        String next = iterator.next();
        if (!cols.contains(next)) {
          query.append(next + "=" + "excluded." + next + ",");
        }
      }
      query.deleteCharAt(query.length() - 1);

    } catch (Exception e) {
      LOGGER.error("Error while trying to buildPgConflictQuery.", e);
      throw new HiveJdbcDatabaseAccessException(e);
    } finally {
      cleanupResources(conn, null, null);
    }
    return query.toString();
  }
}
