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

import com.google.common.collect.Sets;
import com.wdw.hive.jdbchandler.utils.HiveParse;
import com.wdw.hive.jdbchandler.utils.LogUtil;
import com.wdw.hive.jdbchandler.utils.QueryConditionBuilder;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

/**
 * Main configuration handler class
 */
public class JdbcStorageConfigManager {

  public static final String CONFIG_PREFIX = "hive.sql.jdbc";
  //private static final Logger LOGGER = LoggerFactory.getLogger(JdbcStorageConfigManager.class);

  protected static final LogUtil LOGGER = LogUtil.getLogger();

  private JdbcStorageConfigManager() {
  }


  public static void copyConfigurationToJob(Properties props, Map<String, String> jobProps) {
    checkRequiredPropertiesAreDefined(props);

    for (JdbcStorageConfig jdbcStorageConfig : JdbcStorageConfig.values()) {
      if (!props.containsKey(jdbcStorageConfig.getPropertyName())) {
        props.put(jdbcStorageConfig.getPropertyName(), "");
      }
    }
    for (Map.Entry<Object, Object> entry : props.entrySet()) {
      jobProps.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
    }
  }


  public static Configuration convertPropertiesToConfiguration(Properties props) {
    checkRequiredPropertiesAreDefined(props);
    Configuration conf = new Configuration();

    for (Map.Entry<Object, Object> entry : props.entrySet()) {
      conf.set(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
    }

    return conf;
  }

  public static Configuration convertMapToConfiguration(Map<String, String> props) {
    Configuration conf = new Configuration();

    for (Map.Entry<String, String> entry : props.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }

    return conf;
  }

  private static void checkRequiredPropertiesAreDefined(Properties props) {
    DatabaseType dbType = DatabaseType
        .valueOf(props.getProperty(JdbcStorageConfig.DATABASE_TYPE.getPropertyName()));
    CustomConfigManager configManager = CustomConfigManagerFactory
        .getCustomConfigManagerFor(dbType);
    String table = props.getProperty(JdbcStorageConfig.TABLE.getPropertyName());
    String query = props.getProperty(JdbcStorageConfig.QUERY.getPropertyName());
    if (StringUtils.isBlank(table) && StringUtils.isBlank(query)) {
      throw new IllegalArgumentException("hive.sql.table and hive.sql.query must one");
    } else if (StringUtils.isNotBlank(table) && StringUtils.isNotBlank(query)) {
      throw new IllegalArgumentException("hive.sql.table and hive.sql.query must one");
    }
    configManager.checkRequiredProperties(props);
  }


  public static String getConfigValue(JdbcStorageConfig key, Configuration config) {
    return getConfigValue(key, config, null);
  }

  public static String getConfigValue(JdbcStorageConfig key, Configuration config,
      String defaultValue) {
    String res = config.get(key.getPropertyName());
    if (StringUtils.isBlank(res)) {
      return defaultValue;
    } else {
      return res;
    }
  }

  public static Integer getConfigIntValue(JdbcStorageConfig key, Configuration config,
      Integer defaultValue) {
    String value = config.get(key.getPropertyName());
    if (StringUtils.isBlank(value)) {
      return defaultValue;
    } else {
      return Integer.parseInt(value);
    }
  }

  public static Integer getConfigIntValue(JdbcStorageConfig key, Configuration config) {
    String value = config.get(key.getPropertyName());
    if (StringUtils.isBlank(value)) {
      return 0;
    } else {
      return Integer.parseInt(value);
    }
  }

  public static String getQueryToExecute(Configuration config, String tableName) {
    String query = "";
    query = String.format("select * from %s", tableName);
    //把hive中的条件附加到从mysql中读取中来
    String hiveFilterCondition = QueryConditionBuilder.getInstance().buildCondition(config);
    LOGGER.info("hiveFilterCondition:{}", hiveFilterCondition);
    if ((hiveFilterCondition != null) && (!hiveFilterCondition.trim().isEmpty())) {
      query = query + (query.toLowerCase().contains("where") ? " and " : " where ")
          + hiveFilterCondition;
    }
    return query;
  }

  public static String getCountToExecute(Configuration config, String tableName) {
    String query = "";
    query = String.format("select count(1) from %s", tableName);
    //把hive中的条件附加到从mysql中读取中来
    String hiveFilterCondition = QueryConditionBuilder.getInstance().buildCondition(config);
    LOGGER.info("hiveFilterCondition:{}", hiveFilterCondition);
    if ((hiveFilterCondition != null) && (!hiveFilterCondition.trim().isEmpty())) {
      query = query + (query.toLowerCase().contains("where") ? " and " : " where ")
          + hiveFilterCondition;
    }
    return query;
  }

  public static String getQueryToExecute(Configuration config) {
    String query = "";
    String tableName = JdbcStorageConfig.TABLE.getConfigValue(config);
    if (StringUtils.isNotBlank(tableName)) {
      query = String.format("select * from %s", tableName);
    } else {
      query = JdbcStorageConfig.QUERY.getConfigValue(config);
    }
    //把query中的条件()
    if ((query.contains("where") || query.contains("WHERE")) && (query.contains("or") || query
        .contains("OR"))) {
      query = query.replaceAll("(?i)where ", "where (") + ")";
    }
    //把hive中的条件附加到从mysql中读取中来
    String hiveFilterCondition = QueryConditionBuilder.getInstance().buildCondition(config);
    LOGGER.info("hiveFilterCondition:{}", hiveFilterCondition);
    if ((hiveFilterCondition != null) && (!hiveFilterCondition.trim().isEmpty())) {
      query = query + (query.toLowerCase().contains("where") ? " and " : " where ")
          + hiveFilterCondition;
    }
    return query;
  }

  private static String getTableName(String query) {
    if (StringUtils.isBlank(query)) {
      return null;
    }
    HiveParse hp = new HiveParse();
    Map<String, Object> parse = hp.parse(query);
    Set<String> tables = (Set<String>) parse.get("tables");
    if (tables.size() != 1) {
      return null;
    } else {
      return tables.toArray()[0].toString().split("\t")[0];
    }
  }

  public static String getTableName(Configuration conf) {
    String tableName = JdbcStorageConfig.TABLE.getConfigValue(conf);
    if (StringUtils.isBlank(tableName)) {
      tableName = getTableName(JdbcStorageConfig.QUERY.getConfigValue(conf));
    }
    return tableName;
  }


  private static boolean isEmptyString(String value) {
    return ((value == null) || (value.trim().isEmpty()));
  }

  public static void convertPropertiesToConfiguration(Map<String, String> prop,
      Configuration conf) {
    if (prop != null) {
      Iterator<Map.Entry<String, String>> iterator = prop.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<String, String> next = iterator.next();
        conf.set(next.getKey(), next.getValue());
      }
    }
  }

  public static void removeIgnoreColumns(Configuration conf, List<String> columns) {
    String ignoreColumns = JdbcStorageConfig.IGNORE_COLUMN.getConfigValue(conf);
    if (StringUtils.isNotBlank(ignoreColumns)) {
      HashSet<String> set = Sets.newHashSet(ignoreColumns.split(","));
      Iterator<String> iterator = columns.iterator();
      while (iterator.hasNext()) {
        String next = iterator.next();
        if (StringUtils.isNotBlank(next) && set.contains(next)) {
          iterator.remove();
        }
      }
    }
  }

  public static int[] removeIgnoreTypes(Configuration conf, List<String> hiveColumnNames,
      int[] types) {
    String ignoreColumns = JdbcStorageConfig.IGNORE_COLUMN.getConfigValue(conf);
    if (StringUtils.isNotBlank(ignoreColumns)) {
      HashSet<String> set = Sets.newHashSet(ignoreColumns.split(","));
      Iterator<String> iterator = set.iterator();
      int length = types.length;
      while (iterator.hasNext()) {
        String next = iterator.next();
        if (hiveColumnNames.contains(next)) {
          length -= 1;
        }
      }
      int[] resTypes = new int[length];
      if (length != types.length) {
        int index = 0;
        for (int i = 0; i < hiveColumnNames.size(); i++) {
          String name = hiveColumnNames.get(i);
          if (StringUtils.isBlank(name) || !set.contains(name)) {
            resTypes[index] = types[i];
            index += 1;
          }
        }
      }
      return resTypes;
    }
    return types;
  }

  public static Set<Integer> getWriteIgnoreIndex(Configuration conf, List<String> columnNames) {
    String ignoreColumns = JdbcStorageConfig.IGNORE_COLUMN.getConfigValue(conf);
    HashSet<Integer> indexs = Sets.newHashSet();
    if (StringUtils.isNotBlank(ignoreColumns)) {
      HashSet<String> set = Sets.newHashSet(ignoreColumns.split(","));
      for (int i = 0; i < columnNames.size(); i++) {
        String name = columnNames.get(i);
        if (StringUtils.isNotBlank(name) && set.contains(name)) {
          indexs.add(i);
        }
      }
    }
    return indexs;
  }
}
