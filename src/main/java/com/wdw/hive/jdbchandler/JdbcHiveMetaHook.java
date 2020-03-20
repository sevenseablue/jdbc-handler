package com.wdw.hive.jdbchandler;

import com.wdw.hive.jdbchandler.authority.AuthorityManager;
import com.wdw.hive.jdbchandler.conf.JdbcStorageConfig;
import com.wdw.hive.jdbchandler.conf.JdbcStorageConfigManager;
import com.wdw.hive.jdbchandler.dao.DatabaseAccessor;
import com.wdw.hive.jdbchandler.dao.DatabaseAccessorFactory;
import com.wdw.hive.jdbchandler.exception.HiveJdbcAuthorityException;
import com.wdw.hive.jdbchandler.exception.HiveJdbcDatabaseAccessException;
import com.wdw.hive.jdbchandler.spitter.Splitter;
import com.wdw.hive.jdbchandler.utils.Constant;
import com.wdw.hive.jdbchandler.utils.LogUtil;
import com.wdw.hive.jdbchandler.utils.PropUtil;

import java.util.*;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.DefaultHiveMetaHook;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * Created with Lee. Date: 2019/8/16 Time: 10:57 To change this template use File | Settings | File
 * Templates. Description:
 *
 * @author :
 */

public class JdbcHiveMetaHook extends DefaultHiveMetaHook {

  //private final static Logger LOGGER = LoggerFactory.getLogger(JdbcHiveMetaHook.class);
  private static final LogUtil LOGGER = LogUtil.getLogger();
  private static final Pattern PATTERM = Pattern.compile("jdbc:.*?:\\/\\/(.*?):(.*)?\\/(.*)");

  public JdbcHiveMetaHook() {
  }

  @Override
  public void commitInsertTable(Table table, boolean overwrite) throws MetaException {

  }

  @Override
  public void preInsertTable(Table table, boolean overwrite) throws MetaException {
    LOGGER.info("start");
    LOGGER.info("table:{},overwrite:{}" + table.toString(), overwrite);
    Map<String, String> parameters = table.getParameters();

    Configuration conf = new Configuration();
    JdbcStorageConfigManager.convertPropertiesToConfiguration(table.getParameters(), conf);

    if (JdbcStorageConfig.READ_WRITE.getConfigValue(conf) != null && !JdbcStorageConfig.READ_WRITE
        .getConfigValue(conf).contains("write")) {
      throw new MetaException("please check authority");
    }
    if (overwrite) {
      try {
        //增加如果没有table参数不能插入校验
        if (StringUtils.isBlank(table.getParameters().get(JdbcStorageConfig.TABLE.getPropertyName()))) {
          throw new MetaException(
                  "[hive.sql.query] is only query ,if want insert please use [hive.sql.table]");
        }
        String tableName = JdbcStorageConfig.TABLE.getConfigValue(conf);
        LOGGER.info("conf:{},tableName:{}", conf, tableName);

        DatabaseAccessor dbAccessor = DatabaseAccessorFactory.getAccessor(conf);
        String delType = JdbcStorageConfig.DELETE_TYPE.getConfigValue(conf);
        if (delType != null && delType.toLowerCase().equals("table")) {
          dbAccessor.truncateTable(conf, JdbcStorageConfig.TABLE.getConfigValue(conf));
        } else if(delType != null && delType.toLowerCase().equals("column")){
          String cols = JdbcStorageConfig.DELETE_BYCOLUMN.getConfigValue(conf);
          Map<String, String> hiveVars = SessionState.get().getHiveVariables();
          String[] colArr = cols.split(",");
          Map<String, String> delVarsMap = new HashMap<>();
          LOGGER.info(hiveVars);
          for(String col: colArr){
            String colIntervalK = String.format("hive.jdbc.delete.%s.%s.interval", tableName, col);
            String errorMsg = String.format("hivevar:%s must be set by value or interval. for example, by value, set hivevar:%s=2020-01-01, means delete from table where col=2020-01-01. or by interval, separated by\",\", left close, right open, set hivevar:%s=\"2020-01-01,2020-01-02\", means delete from table where col>=2020-01-01 and col<2020-01-02. for now, only timestamp, varchar, int supported for the delete col type;", colIntervalK, colIntervalK, colIntervalK);
            if((!hiveVars.containsKey(colIntervalK))){
              throw new MetaException(errorMsg);
            }
            String colIntervalV = hiveVars.get(colIntervalK).replaceAll("^\"", "").replaceAll("\"$", "");
            colIntervalV = colIntervalV.trim();
            if(colIntervalV.equals("")){
              throw new MetaException(errorMsg);
            }else {
              String[] colIntervalVArr = colIntervalV.split(",");
              if(colIntervalVArr.length==1 || colIntervalVArr.length==2) {
                delVarsMap.put(colIntervalK, colIntervalV);
              }else{
                throw new MetaException(errorMsg);
              }
            }
          }
          dbAccessor.deleteRows(conf, JdbcStorageConfig.TABLE.getConfigValue(conf), colArr, delVarsMap);
        }
      } catch (HiveJdbcDatabaseAccessException e) {
        throw new MetaException(e.getMessage());
      }
    }

    LOGGER.info("end");
  }

  @Override
  public void rollbackInsertTable(Table table, boolean overwrite) throws MetaException {

  }

  @Override
  public void preCreateTable(Table table) throws MetaException {
    Map<String, String> parameters = table.getParameters();
    Configuration conf = JdbcStorageConfigManager.convertMapToConfiguration(parameters);
    LOGGER.info("table : {}", table.toString());

    //验证是否有权限，
    String power = null;
    try {
      power = AuthorityManager.getAuthority(conf);
      LOGGER.info("power"+power);
    } catch (HiveJdbcAuthorityException e) {
      throw new MetaException(e.getMessage());
    }
    if (StringUtils.isBlank(power)) {
      throw new MetaException("no authority");
    }
    if (power.contains("insert")){
      parameters.put(JdbcStorageConfig.USERACCOUNT.getPropertyName(), "rw");
    } else{
      parameters.put(JdbcStorageConfig.USERACCOUNT.getPropertyName(), "r");
    }
    conf = JdbcStorageConfigManager.convertMapToConfiguration(parameters);

    parameters.put(JdbcStorageConfig.PASSWORD.getPropertyName(), Constant.STAR6);

    // write authority and config about table parameters.
    boolean writable =  JdbcStorageConfig.READ_WRITE.getConfigValue(conf).contains("write");
    if(writable) {
      if (!power.contains("insert")){
        throw new MetaException("have not the write privilege");
      }

      String delType = JdbcStorageConfig.DELETE_TYPE.getConfigValue(conf);
      if (delType == null || (!delType.toLowerCase().equals("table") && !delType.toLowerCase().equals("column"))) {
        throw new MetaException("hive.sql.jdbc.delete.type must not be null, and it's value must be in (table, column);");
      }
      String delByCol = JdbcStorageConfig.DELETE_BYCOLUMN.getConfigValue(conf);
      if (delType.toLowerCase().equals("column") && (delByCol == null)) {
        throw new MetaException("when hive.sql.jdbc.delete.type is column, hive.sql.jdbc.delete.by.columns must not be null;");
      }

      String part_tables =  JdbcStorageConfig.PART_TABLES.getConfigValue(conf, "");
      if(part_tables != null && !part_tables.equals("") && Splitter.getTbNames(part_tables).size()>1){
        throw new MetaException("when a table can be written , part.tables can not be config.");
      }
    }

    // 校验分区表和query条件
    if (table.getPartitionKeys() != null && table.getPartitionKeys().size() != 0) {
      throw new MetaException("jdbc not support partitioned");
    }
    String query = JdbcStorageConfig.QUERY.getConfigValue(conf);
    if (StringUtils.isNotBlank(query) && (query.contains(" join ") || query
        .contains(" group by "))) {
      throw new MetaException("hive.sql.query not support [join],[group by]");
    }

    // 校验切分字段是否存在。
    String partitionColumn = JdbcStorageConfig.PARTITION_COLUMN.getConfigValue(conf);
    DatabaseAccessor accessor = DatabaseAccessorFactory.getAccessor(conf);
    Pair<String, Integer> pair = null;
    try {
      pair = accessor.convertPartitionColumn(partitionColumn, conf);
    } catch (HiveJdbcDatabaseAccessException e) {
      e.printStackTrace();
    }
    if (StringUtils.isBlank(partitionColumn) && pair == null) {
      LOGGER.warn("hive.sql.jdbc.partition.column is blank and no match primary key");
      String flag = PropUtil.getProperty("hive.create.table.nosplit.column", "false");
      if (!flag.equals("true")) {
        throw new MetaException("hive.sql.jdbc.partition.column is blank and no match primary key");
      }
    }
    if (StringUtils.isNotBlank(partitionColumn) && pair == null) {
      throw new MetaException(
          "please check param [hive.sql.jdbc.partition.column],this column must index and type must in [number,date,timestamp]");
    }

    //验证切分size不得小于默认值
    int splitSize = JdbcStorageConfig.AUTO_SPLIT_SIZE
        .getConfigIntValue(conf, Constant.DEFAULT_SPLIT_SIZE);
    if (splitSize < Constant.DEFAULT_SPLIT_SIZE) {
      throw new MetaException("split size must >= 10W");
    }

    //校验query字段和hive字段是否对应
    try {
      List<String> columnNames = accessor.getColumnNames(conf);
      List<FieldSchema> cols = table.getSd().getCols();
      Set<String> columnNamesSet = new HashSet<>();
      for(String col: columnNames){
        columnNamesSet.add(col.toLowerCase());
      }

      for (int i = 0; i < cols.size(); i++) {
        if (!columnNamesSet.contains(cols.get(i).getName().toLowerCase())) {
          throw new MetaException(String
              .format("rmdb column:%s no match hive column:%s", columnNames.get(i),
                  cols.get(i).getName()));
        }
      }
    } catch (HiveJdbcDatabaseAccessException e) {
      e.printStackTrace();
    }

  }

  @Override
  public void rollbackCreateTable(Table table) throws MetaException {
  }

  @Override
  public void commitCreateTable(Table table) throws MetaException {
  }

  @Override
  public void preDropTable(Table table) throws MetaException {
  }

  @Override
  public void rollbackDropTable(Table table) throws MetaException {
  }

  @Override
  public void commitDropTable(Table table, boolean deleteData) throws MetaException {
  }
}
