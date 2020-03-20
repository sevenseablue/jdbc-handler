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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.wdw.db.resource.MasterDelegatorDataSource;
import com.wdw.db.resource.RWDelegatorDataSource;
import com.wdw.db.resource.ReadDelegatorDataSource;
import com.wdw.db.resource.SlaveDelegatorDataSource;
import com.wdw.hive.jdbchandler.JdbcInputSplit;
import com.wdw.hive.jdbchandler.conf.JdbcStorageConfig;
import com.wdw.hive.jdbchandler.conf.JdbcStorageConfigManager;
import com.wdw.hive.jdbchandler.exception.HiveJdbcDatabaseAccessException;
import com.wdw.hive.jdbchandler.utils.Constant;
import com.wdw.hive.jdbchandler.utils.HiveJdbcBridgeUtils;
import com.wdw.hive.jdbchandler.utils.LogUtil;
import com.wdw.hive.jdbchandler.utils.Tuple2;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.sql.DataSource;
import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;

/**
 * A data accessor that should in theory work with all JDBC compliant database drivers.
 */
public class GenericJdbcDatabaseAccessor implements DatabaseAccessor {

  protected static final String DBCP_CONFIG_PREFIX =
      JdbcStorageConfigManager.CONFIG_PREFIX + ".dbcp";
  //protected static final Logger LOGGER = LoggerFactory.getLogger(GenericJdbcDatabaseAccessor.class);
  protected static final LogUtil LOGGER = LogUtil.getLogger();
  public DataSource dataSource = null;
  static final Pattern fromPattern = Pattern
      .compile("(.*?\\sfrom\\s)(.*+)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
  static final Pattern wherePattern = Pattern
      .compile("(.*?\\swhere\\s)(.*+)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  public GenericJdbcDatabaseAccessor() {
  }


  @Override
  public List<String> getColumnNames(Configuration conf) throws HiveJdbcDatabaseAccessException {
    Connection conn = null;
    PreparedStatement ps = null;
    ResultSet rs = null;

    try {
      initializeDatabaseConnection(conf);
      String sql = JdbcStorageConfigManager.getQueryToExecute(conf);
      String metadataQuery = addLimitToQuery(sql, 1);
      LOGGER.info("sql:[{}]", metadataQuery);

      conn = dataSource.getConnection();
      ps = conn.prepareStatement(metadataQuery);
      rs = ps.executeQuery();

      ResultSetMetaData metadata = rs.getMetaData();
      int numColumns = metadata.getColumnCount();
      List<String> columnNames = new ArrayList<String>(numColumns);
      for (int i = 0; i < numColumns; i++) {
        columnNames.add(metadata.getColumnName(i + 1));
      }

      return columnNames;
    } catch (Exception e) {
      LOGGER.error("Error while trying to get column names.", e);
      throw new HiveJdbcDatabaseAccessException(
          "Error while trying to get column names: " + e.getMessage(), e);
    } finally {
      cleanupResources(conn, ps, rs);
    }

  }


  @Override
  public int getTotalNumberOfRecords(Configuration conf) throws HiveJdbcDatabaseAccessException {
    Connection conn = null;
    PreparedStatement ps = null;
    ResultSet rs = null;

    try {
      initializeDatabaseConnection(conf);
      String sql = JdbcStorageConfigManager.getQueryToExecute(conf);
      //改成截取from之后的字段，然后前面拼接select count()
      Matcher m = fromPattern.matcher(sql);
      Preconditions.checkArgument(m.matches());
      String queryAfterFrom = " " + m.group(2) + " ";
      String countQuery = "SELECT COUNT(*) from" + queryAfterFrom;
      LOGGER.info("query:[{}]", countQuery);

      conn = dataSource.getConnection();
      ps = conn.prepareStatement(countQuery);
      long start = System.currentTimeMillis();
      rs = ps.executeQuery();
      if (rs.next()) {
        long end = System.currentTimeMillis();
        LOGGER.info("query total num record use time is : {}s", (end - start) / 1000);
        return rs.getInt(1);
      } else {
        LOGGER.warn("The count query did not return any results.", countQuery);
        throw new HiveJdbcDatabaseAccessException("Count query did not return any results.");
      }
    } catch (HiveJdbcDatabaseAccessException he) {
      throw he;
    } catch (Exception e) {
      LOGGER.error("Caught exception while trying to get the number of records", e);
      throw new HiveJdbcDatabaseAccessException(e);
    } finally {
      cleanupResources(conn, ps, rs);
    }
  }

  @Override
  public int getTotalNumberOfRecords(Configuration conf, String tbName) throws HiveJdbcDatabaseAccessException {
    Connection conn = null;
    PreparedStatement ps = null;
    ResultSet rs = null;

    try {
      initializeDatabaseConnection(conf);
      String sql = JdbcStorageConfigManager.getCountToExecute(conf, tbName);
      LOGGER.info("query:[{}]", sql);

      conn = dataSource.getConnection();
      ps = conn.prepareStatement(sql);
      long start = System.currentTimeMillis();
      rs = ps.executeQuery();
      if (rs.next()) {
        long end = System.currentTimeMillis();
        LOGGER.info("query total num record use time is : {}s", (end - start) / 1000);
        return rs.getInt(1);
      } else {
        LOGGER.warn("The count query did not return any results.", sql);
        throw new HiveJdbcDatabaseAccessException("Count query did not return any results.");
      }
    } catch (HiveJdbcDatabaseAccessException he) {
      throw he;
    } catch (Exception e) {
      LOGGER.error("Caught exception while trying to get the number of records", e);
      throw new HiveJdbcDatabaseAccessException(e);
    } finally {
      cleanupResources(conn, ps, rs);
    }
  }


  /*@Override
  public JdbcRecordIterator getRecordIterator(Configuration conf, int limit, int offset) throws HiveJdbcDatabaseAccessException {

    Connection conn = null;
    PreparedStatement ps = null;
    ResultSet rs = null;

    try {
      initializeDatabaseConnection(conf);

      LOGGER.info("limit={},offset={}",limit,offset);
      String sql = JdbcStorageConfigManager.getQueryToExecute(conf);
      String limitQuery = addLimitAndOffsetToQuery(sql, limit, offset);
      LOGGER.info("Query to execute is [{}]", limitQuery);

      conn = dataSource.getConnection();
      ps = conn.prepareStatement(limitQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      ps.setFetchSize(getFetchSize(conf));
      rs = ps.executeQuery();
	  LOGGER.info("fetchsize:{}",getFetchSize(conf));
      return new JdbcRecordIterator(conn, ps, rs);
    }
    catch (Exception e) {
      LOGGER.error("Caught exception while trying to execute query", e);
      cleanupResources(conn, ps, rs);
      throw new HiveJdbcDatabaseAccessException("Caught exception while trying to execute query", e);
    }
  }*/
  @Override
  public JdbcRecordIterator
  getRecordIterator(Configuration conf, JdbcInputSplit split)
      throws HiveJdbcDatabaseAccessException {

    Connection conn = null;
    PreparedStatement ps = null;
    ResultSet rs = null;

    try {
      initializeDatabaseConnection(conf);
   /* String tableName = JdbcStorageConfigManager.getTableName(conf);
	  String sql = JdbcStorageConfigManager.getQueryToExecute(conf);
	  String partitionQuery;
	  LOGGER.info("record reader origin sql is [{}]",sql);
	  if (split.getPartitionColumn() != null) {
		partitionQuery = addBoundaryToQuery(sql, split.getPartitionColumn(), split.getLowerBound(), split.getUpperBound());
	  } else {
		partitionQuery = addLimitAndOffsetToQuery(sql, split.getLimit(), split.getOffset());
	  }
	  LOGGER.info("record reader sql is [{}]", partitionQuery);*/

      LOGGER.info("record reader sql is [{}]", split.getQuery());
      conn = dataSource.getConnection();
      ps = conn.prepareStatement(split.getQuery(), ResultSet.TYPE_FORWARD_ONLY,
          ResultSet.CONCUR_READ_ONLY);
      ps.setFetchSize(getFetchSize(conf));
      long start = System.currentTimeMillis();
      rs = ps.executeQuery();
      long end = System.currentTimeMillis();
      LOGGER.info("record reader cost time is :[{}]", (end - start) / 1000);

      return new JdbcRecordIterator(conn, ps, rs, conf);
    } catch (Exception e) {
      LOGGER.error("Caught exception while trying to execute query", e);
      cleanupResources(conn, ps, rs);
      throw new HiveJdbcDatabaseAccessException(
          "Caught exception while trying to execute query:" + e.getMessage(), e);
    }
  }

  private String addBoundaryToQuery(String sql, String partitionColumn, String lowerBound,
      String upperBound) {
    if (StringUtils.isBlank(lowerBound) && StringUtils.isBlank(upperBound)) {
      return sql;
    }
    StringBuilder boundary = new StringBuilder();
    boundary.append("(");
    if (lowerBound != null) {
      boundary.append(String.format("%s >= '%s' ", partitionColumn, lowerBound));
    }
    if (upperBound != null) {
      if (lowerBound != null) {
        boundary.append(" and ");
      }
      boundary.append(String.format("%s < '%s' ", partitionColumn, upperBound));
    }
    if (lowerBound == null && upperBound != null) {
      boundary.append(String.format(" or %s is null", partitionColumn));
    }
    boundary.append(")");
    return sql + (sql.toLowerCase().contains("where") ? " and " : " where ") + boundary.toString();
  }

  /*protected String addBoundaryToQuery(String tableName, String sql, String partitionColumn, String lowerBound,
									  String upperBound) {
	String boundaryQuery;
	lowerBound = lowerBound == null ? null :"'" + lowerBound + "'";
	upperBound = upperBound == null ? null :"'" + upperBound + "'";
	if (tableName != null) {
	  boundaryQuery = "SELECT * FROM " + tableName + " WHERE ";
	} else {
	  boundaryQuery = "SELECT * FROM (" + sql + ") tmptable WHERE ";
	}
	if (lowerBound != null) {
	  boundaryQuery += partitionColumn + " >= " + lowerBound;
	}
	if (upperBound != null) {
	  if (lowerBound != null) {
		boundaryQuery += " AND ";
	  }
	  boundaryQuery += partitionColumn + " < " + upperBound;
	}
	if (lowerBound == null && upperBound != null) {
	  boundaryQuery += " OR " + partitionColumn + " IS NULL";
	}
	String result;
	if (tableName != null) {
	  // Looking for table name in from clause, replace with the boundary query
	  // TODO consolidate this
	  // Currently only use simple string match, this should be improved by looking
	  // for only table name in from clause
	  String tableString = null;
	  Matcher m = fromPattern.matcher(sql);
	  Preconditions.checkArgument(m.matches());
	  String queryBeforeFrom = m.group(1);
	  String queryAfterFrom = " " + m.group(2) + " ";

	  Character[] possibleDelimits = new Character[] {'`', '\"', ' '};
	  for (Character possibleDelimit : possibleDelimits) {
		if (queryAfterFrom.contains(possibleDelimit + tableName + possibleDelimit)) {
		  tableString = possibleDelimit + tableName + possibleDelimit;
		  break;
		}
	  }
	  if (tableString == null) {
		throw new RuntimeException("Cannot find " + tableName + " in sql query " + sql);
	  }
	  result = queryBeforeFrom + queryAfterFrom.replace(tableString, " (" + boundaryQuery + ") " + tableName + " ");
	} else {
	  result = boundaryQuery;
	}
	return result;
  }*/


  @Override
  public Tuple2<Connection, PreparedStatement> buildConnect(Configuration conf)
      throws HiveJdbcDatabaseAccessException {
    Tuple2<Connection, PreparedStatement> tup = null;
    try {
      String tableName = JdbcStorageConfig.TABLE.getConfigValue(conf);
      if (StringUtils.isBlank(tableName)) {
        throw new HiveJdbcDatabaseAccessException(
            "output table is null,plase create table add conf :hive.sql.table=table_name");
      }

      List<String> columnNames = this.getColumnNames(conf);
      String query = buildQuery(conf, tableName, columnNames);
      //建立连接
      try {
        initializeDatabaseConnection(conf);
      } catch (Exception e) {
        e.printStackTrace();
      }
      Connection conn = dataSource.getConnection();
      conn.setAutoCommit(false);
      PreparedStatement statement = conn.prepareStatement(query);
      tup = new Tuple2<>(conn, statement);
    } catch (Exception e) {
      LOGGER.error("buildConnect is failed:", e);
      throw new HiveJdbcDatabaseAccessException("Caught exception while trying to execute query",
          e);
    }

    return tup;
  }

  @Override
  public void truncateTable(Configuration conf, String tableName)
      throws HiveJdbcDatabaseAccessException {
    Connection conn = null;
    PreparedStatement statement = null;
    try {
      initializeDatabaseConnection(conf);
      conn = dataSource.getConnection();
      String sql = String.format("truncate table %s", tableName);
      LOGGER.info("truncateTable sql:[{}]", sql);
      statement = conn.prepareStatement(sql);
      statement.execute();
    } catch (Exception e) {
      LOGGER.error("truncateTable failed : ", e);
      throw new HiveJdbcDatabaseAccessException("truncateTable failed", e);
    } finally {
      cleanupResources(conn, statement, null);
    }
  }

  @Override
  public boolean deleteRows(Configuration conf, String tableName, String[] cols, Map<String, String> hiveVars) throws HiveJdbcDatabaseAccessException {
    List<String> colNames = getColumnNames(conf);
    int[] colTypes = getFieldTypes(conf);
    LOGGER.info(colNames);
    LOGGER.info(cols);
    LOGGER.info(colTypes);
    Map<String, Integer> nameToType = new HashMap<>();
    int ind = 0;
    for(String col: colNames){
      nameToType.put(col.toLowerCase(), colTypes[ind]);
      ind += 1;
    }

    Connection conn = null;
    PreparedStatement statement = null;
    try {
      initializeDatabaseConnection(conf);
      conn = dataSource.getConnection();
      List<Object> values = Lists.newArrayList();
      StringBuffer sql = new StringBuffer();
      sql.append(String.format("delete from %s where ", tableName));

      for(String col: cols) {
        String colIntervalK = String.format("hive.jdbc.delete.%s.%s.interval", tableName, col);
        String colIntervalV = hiveVars.get(colIntervalK);
        String[] colIntervalVArr = colIntervalV.split(",");
        if(colIntervalVArr.length==1){
          sql.append(String.format(" %s = ? and ", col));
          values.add(HiveJdbcBridgeUtils.readObject(colIntervalVArr[0], nameToType.getOrDefault(col.toLowerCase(), Types.VARCHAR)));
        }else if(colIntervalVArr.length==2){
          sql.append(String.format(" %s >= ? and %s < ? and ", col, col));
          values.add(HiveJdbcBridgeUtils.readObject(colIntervalVArr[0], nameToType.getOrDefault(col.toLowerCase(), Types.VARCHAR)));
          values.add(HiveJdbcBridgeUtils.readObject(colIntervalVArr[1], nameToType.getOrDefault(col.toLowerCase(), Types.VARCHAR)));
        }
      }
      statement = conn.prepareStatement(sql.toString().replaceAll("and $", ""));
      for (int i = 0; i < values.size(); i++) {
        statement.setObject(i + 1, values.get(i));
      }

      LOGGER.info("deleteRows sql:[{}]", statement);
      statement.execute();
    } catch (Exception e) {
      LOGGER.error("deleteRows failed : ", e);
      throw new HiveJdbcDatabaseAccessException("deleteRows failed", e);
    } finally {
      cleanupResources(conn, statement, null);
    }
    return true;
  }

  @Override
  public int[] getFieldTypes(Configuration conf) throws HiveJdbcDatabaseAccessException {

    int[] types = null;

    Connection conn = null;
    PreparedStatement ps = null;
    ResultSet rs = null;

    try {
      initializeDatabaseConnection(conf);
      String sql = JdbcStorageConfigManager.getQueryToExecute(conf);
      String metadataQuery = addLimitToQuery(sql, 1);
      LOGGER.info("query:[{}]", metadataQuery);

      conn = dataSource.getConnection();
      ps = conn.prepareStatement(metadataQuery);
      rs = ps.executeQuery();
      ResultSetMetaData metadata = rs.getMetaData();

      int columnCount = metadata.getColumnCount();
      types = new int[columnCount];

      for (int i = 0; i < columnCount; i++) {
        types[i] = (metadata.getColumnType(i + 1));
      }

    } catch (Exception e) {
      LOGGER.error("Error while trying to get column types.", e);
      throw new HiveJdbcDatabaseAccessException(
          "Error while trying to get column types: " + e.getMessage(), e);
    } finally {
      cleanupResources(conn, ps, rs);
    }
    return types;
  }

  @Override
  public Connection getConnection(Configuration conf) throws HiveJdbcDatabaseAccessException {
    try {
      initializeDatabaseConnection(conf);
      return this.dataSource.getConnection();
    } catch (Exception e) {
      LOGGER.error("failed get conn", e);
    }
    return null;
  }

  @Override
  public Pair<String, Integer> convertPartitionColumn(String partitionColumn, Configuration conf)
      throws HiveJdbcDatabaseAccessException {
    Connection conn = null;
    Pair<String, Integer> pair = null;
    try {
      initializeDatabaseConnection(conf);
      conn = this.dataSource.getConnection();
      DatabaseMetaData metaData = conn.getMetaData();
      String tableName = JdbcStorageConfigManager.getTableName(conf);

      if (StringUtils.isBlank(partitionColumn)) {
        LOGGER.info("partitionColumn is blank , query primary key as partitionColumn");
        ResultSet primaryKeys = metaData.getPrimaryKeys(null, null, tableName);
        if (primaryKeys.next()) {
          String id = primaryKeys.getString("COLUMN_NAME");
          ResultSet columns = metaData.getColumns(null, null, tableName, id);
          columns.next();
          int dataType = columns.getInt("DATA_TYPE");
          if (dataType == Types.BIGINT || dataType == Types.INTEGER) {
            pair = new ImmutablePair<>(id, dataType);
          }
        }
      } else {
        LOGGER.info("partitionColumn is not  blank , query partitionColumn is or not index");
        ResultSet indexInfo = metaData.getIndexInfo(null, null, tableName, false, false);
        while (indexInfo.next()) {
          String columnName = indexInfo.getString("COLUMN_NAME");
          if (partitionColumn.equals(columnName)) {
            ResultSet columns = metaData.getColumns(null, null, tableName, partitionColumn);
            columns.next();
            int dataType = columns.getInt("DATA_TYPE");
            if (dataType == Types.BIGINT || dataType == Types.INTEGER || dataType == Types.TIMESTAMP
                || dataType == Types.DATE || dataType == Types.DOUBLE) {
              pair = new ImmutablePair<>(partitionColumn, dataType);
            }
            break;
          }
        }
      }
    } catch (Exception e) {
      LOGGER.error("convert partition column failed", e);
      throw new HiveJdbcDatabaseAccessException("convert partition column failed");
    } finally {
      cleanupResources(conn, null, null);
    }
    if (pair == null) {
      LOGGER.info("no match partition column");
    }
    return pair;
  }

  @Override
  public Pair<String, String> getBounds(Configuration conf, String partitionColumn)
      throws HiveJdbcDatabaseAccessException {
    Connection conn = null;
    PreparedStatement ps = null;
    ResultSet rs = null;

    try {
      initializeDatabaseConnection(conf);
      StringBuilder query = new StringBuilder();
      String sql = JdbcStorageConfigManager.getQueryToExecute(conf);
      query.append("SELECT ");
      query.append("MIN(" + partitionColumn + ")");
      query.append(",");
      query.append("MAX(" + partitionColumn + ")");
      Matcher m = fromPattern.matcher(sql);
      Preconditions.checkArgument(m.matches());
      String queryAfterFrom = " " + m.group(2) + " ";
      query.append(" FROM ");
      query.append(queryAfterFrom);
      if (queryAfterFrom.toLowerCase().contains("where")) {
        query.append(" and " + partitionColumn + " IS NOT NULL");
      } else {
        query.append(" where " + partitionColumn + " IS NOT NULL");
      }
      //query.append(" FROM (" + sql + ") tmptable " + "WHERE "+partitionColumn+" IS NOT NULL");
      LOGGER.info("query:MIN/MAX Query to execute is [{}]", query.toString());

      conn = dataSource.getConnection();
      ps = conn.prepareStatement(query.toString());
      rs = ps.executeQuery();
      String lower = null, upper = null;
      if (rs.next()) {
        lower = rs.getString(1);
        upper = rs.getString(2);
        return new ImmutablePair<>(lower, upper);
      } else {
        LOGGER.warn("The count query did not return any results.", query.toString());
        throw new HiveJdbcDatabaseAccessException("MIN/MAX query did not return any results.");
      }

    } catch (HiveJdbcDatabaseAccessException he) {
      throw he;
    } catch (Exception e) {
      LOGGER.error("Caught exception while trying to get MIN/MAX of " + partitionColumn, e);
      throw new HiveJdbcDatabaseAccessException(e);
    } finally {
      cleanupResources(conn, ps, rs);
    }
  }

  @Override
  public DataSource getDataSouce(Configuration conf) throws HiveJdbcDatabaseAccessException {
    try {
      initializeDatabaseConnection(conf);
    } catch (Exception e) {
      throw new HiveJdbcDatabaseAccessException(e);
    }
    return this.dataSource;
  }

  @Override
  public void close() {
    try {
      if (this.dataSource instanceof SlaveDelegatorDataSource) {
        ((SlaveDelegatorDataSource) this.dataSource).close();
        this.dataSource = null;
      }
      if (this.dataSource instanceof MasterDelegatorDataSource) {
        ((MasterDelegatorDataSource) this.dataSource).close();
        this.dataSource = null;
      }
      if (this.dataSource instanceof ReadDelegatorDataSource) {
        ((ReadDelegatorDataSource) this.dataSource).close();
        this.dataSource = null;
      }
      if (this.dataSource instanceof RWDelegatorDataSource) {
        ((RWDelegatorDataSource) this.dataSource).close();
        this.dataSource = null;
      }
    } catch (IOException e) {
      LOGGER.error("close failed", e);
    }

  }

  @Override
  public Pair<String, Long> getNullQuery(Configuration conf, String column)
      throws HiveJdbcDatabaseAccessException {
    Connection conn = null;
    PreparedStatement ps = null;
    ResultSet rs = null;

    Pair<String, Long> res = null;
    try {
      initializeDatabaseConnection(conf);
      String sql = JdbcStorageConfigManager.getQueryToExecute(conf);
      String query =
          sql + (sql.toLowerCase().contains("where") ? " and " : " where ") + column + " is null";
      sql = query.replaceAll("select .*? from", "select count(1) tt from");
      LOGGER.info("query partition column is null nums sql is : [{}]", sql);
      conn = dataSource.getConnection();
      ps = conn.prepareStatement(sql);
      rs = ps.executeQuery();
      if (rs.next()) {
        long sum = rs.getLong("tt");
        if (sum > 0) {
          res = new ImmutablePair<>(query, sum);
        }
      }
    } catch (Exception e) {
      throw new HiveJdbcDatabaseAccessException(e);
    } finally {
      cleanupResources(conn, ps, rs);
    }

    return res;
  }

  @Override
  public String buildQuery(Configuration jc, String table, List<String> columnNames)
      throws HiveJdbcDatabaseAccessException {
    if (columnNames == null) {
      throw new IllegalArgumentException("Field names may not be null");
    }
    JdbcStorageConfigManager.removeIgnoreColumns(jc, columnNames);
    StringBuilder query = new StringBuilder();
    query.append("INSERT INTO ").append(table);
    query.append(" (");
    query.append(StringUtils.join(columnNames, ","));
    query.append(")");
    query.append(" VALUES (");
    for (int i = 0; i < columnNames.size(); i++) {
      query.append("?,");
    }
    query.deleteCharAt(query.length() - 1);
    query.append(");");
    LOGGER.info("insert sql : [{}]", query);
    return query.toString();
  }


  /**
   * Uses generic JDBC escape functions to add a limit and offset clause to a query string
   */
  protected String addLimitAndOffsetToQuery(String sql, int limit, int offset) {
    if (offset == 0) {
      return addLimitToQuery(sql, limit);
    } else {
      return sql + " {LIMIT " + limit + " OFFSET " + offset + "}";
    }
  }


  /*
   * Uses generic JDBC escape functions to add a limit clause to a query string
   */
  protected String addLimitToQuery(String sql, int limit) {
    return sql + " {LIMIT " + limit + "}";
  }


  protected void cleanupResources(Connection conn, PreparedStatement ps, ResultSet rs) {
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

    this.close();

  }

  protected void initializeDatabaseConnection(Configuration conf) throws Exception {
    if (dataSource == null) {
      synchronized (this) {
        if (dataSource == null) {
          Properties props = getConnectionPoolProperties(conf);
          dataSource = BasicDataSourceFactory.createDataSource(props);
        }
      }
    }
  }


  protected Properties getConnectionPoolProperties(Configuration conf) {
    // Create the default properties object
    Properties dbProperties = getDefaultDBCPProperties();

    // override with user defined properties
    Map<String, String> userProperties = conf.getValByRegex(DBCP_CONFIG_PREFIX + "\\.*");
    if ((userProperties != null) && (!userProperties.isEmpty())) {
      for (Entry<String, String> entry : userProperties.entrySet()) {
        dbProperties
            .put(entry.getKey().replaceFirst(DBCP_CONFIG_PREFIX + "\\.", ""), entry.getValue());
      }
    }

    //username password, change to the big account here
    dbProperties.put("username", JdbcStorageConfig.USERNAME.getUserName(conf));
    dbProperties.put("password", JdbcStorageConfig.PASSWORD.getPassword(conf));

    // essential properties that shouldn't be overridden by users
    String url = JdbcStorageConfig.JDBC_URL.getConfigValue(conf);
    if (!url.toLowerCase().contains("useoldaliasmetadatabehavior")) {
      url = url + (url.contains("?") ? "&useOldAliasMetadataBehavior=true"
          : "?useOldAliasMetadataBehavior=true");
    } else if (url.toLowerCase().contains("useoldaliasmetadatabehavior=false")) {
      url = url
          .replaceAll("(?i)useoldaliasmetadatabehavior=false", "useOldAliasMetadataBehavior=true");
    }
    dbProperties.put("url", url);
    dbProperties.put("driverClassName", JdbcStorageConfig.JDBC_DRIVER_CLASS.getConfigValue(conf));
    dbProperties.put("type", "javax.sql.DataSource");
    return dbProperties;
  }


  protected Properties getDefaultDBCPProperties() {
    Properties props = new Properties();
    props.put("initialSize", "1");
    props.put("maxActive", "1");
    props.put("maxIdle", "0");
    props.put("maxWait", "10000");
    props.put("timeBetweenEvictionRunsMillis", "30000");
    return props;
  }


  protected int getFetchSize(Configuration conf) {
    return JdbcStorageConfig.JDBC_FETCH_SIZE.getConfigIntValue(conf, Constant.DEFAULT_FETCH_SIZE);
  }
}
