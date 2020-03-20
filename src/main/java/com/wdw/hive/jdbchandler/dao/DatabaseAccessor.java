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

import com.wdw.hive.jdbchandler.JdbcInputSplit;
import com.wdw.hive.jdbchandler.exception.HiveJdbcDatabaseAccessException;
import com.wdw.hive.jdbchandler.utils.Tuple2;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;

public interface DatabaseAccessor {

  List<String> getColumnNames(Configuration conf) throws HiveJdbcDatabaseAccessException;

  int getTotalNumberOfRecords(Configuration conf) throws HiveJdbcDatabaseAccessException;

  int getTotalNumberOfRecords(Configuration conf, String table) throws HiveJdbcDatabaseAccessException;

  //JdbcRecordIterator getRecordIterator(Configuration conf, int limit, int offset) throws HiveJdbcDatabaseAccessException;

  JdbcRecordIterator getRecordIterator(Configuration conf, JdbcInputSplit inputSplit)
      throws HiveJdbcDatabaseAccessException;

  Tuple2<Connection, PreparedStatement> buildConnect(Configuration conf)
      throws HiveJdbcDatabaseAccessException;

  String buildQuery(Configuration conf, String table, List<String> columnNames)
      throws HiveJdbcDatabaseAccessException;

  void truncateTable(Configuration conf, String tableName) throws HiveJdbcDatabaseAccessException;

  boolean deleteRows(Configuration conf, String tableName, String[] cols, Map<String, String> hiveVars) throws HiveJdbcDatabaseAccessException;

  int[] getFieldTypes(Configuration conf) throws HiveJdbcDatabaseAccessException;

  Connection getConnection(Configuration conf) throws HiveJdbcDatabaseAccessException;

  //String getPrimaryColumn(Configuration conf);

  Pair<String, Integer> convertPartitionColumn(String partitionColumn, Configuration conf)
      throws HiveJdbcDatabaseAccessException;

  Pair<String, String> getBounds(Configuration conf, String partitionColumn)
      throws HiveJdbcDatabaseAccessException;

  DataSource getDataSouce(Configuration conf) throws HiveJdbcDatabaseAccessException;

  void close();

  Pair<String, Long> getNullQuery(Configuration conf, String column)
      throws HiveJdbcDatabaseAccessException;
}
