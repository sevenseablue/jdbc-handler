package com.wdw.hive.jdbchandler.authority;

import com.google.common.collect.Lists;
import com.wdw.hive.jdbchandler.authority.entry.Column;
import com.wdw.hive.jdbchandler.authority.entry.Table;
import com.wdw.hive.jdbchandler.exception.HiveJdbcAuthorityException;
import com.wdw.hive.jdbchandler.utils.LogUtil;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with Lee. Date: 2019/9/25 Time: 15:00 To change this template use File | Settings | File
 * Templates. Description:
 *
 * @author :
 */

public class AuthorityDao {
  private final static LogUtil LOGGER = LogUtil.getLogger();
  public static <T> T queryOne(Connection connection, T authorityInfo) throws HiveJdbcAuthorityException {

    if (authorityInfo == null) {
      throw new HiveJdbcAuthorityException("query class is null");
    }

    List<T> ts = query(connection, authorityInfo);
    if(ts.size()==1) {
      return (T) authorityInfo;
    }
    else {
      throw new HiveJdbcAuthorityException("not one row");
    }
  }

  public static <T> List<T> query(Connection connection, T t) throws HiveJdbcAuthorityException {
    if (t == null) {
      throw new HiveJdbcAuthorityException("query class is null");
    }

    PreparedStatement statement = null;
    ResultSet resultSet = null;
    List<T> result = new ArrayList<>();

    try {

      StringBuffer query = new StringBuffer();
      Table table = t.getClass().getAnnotation(Table.class);
      query.append("select * from ");
      query.append(table.value());
      query.append(" where");

      Field[] fields = t.getClass().getDeclaredFields();
      List<Object> values = Lists.newArrayList();
      for (Field field : fields) {
        field.setAccessible(true);
        Column column = field.getAnnotation(Column.class);
        Object value = field.get(t);
        if (value != null) {
          query.append(" " + column.value() + " = ? and");
          values.add(value);
        }
      }
      query.append(" 1 = 1");

      statement = connection.prepareStatement(query.toString());
      for (int i = 0; i < values.size(); i++) {
        statement.setObject(i + 1, values.get(i));
      }
      resultSet = statement.executeQuery();

      while (resultSet.next()) {
        T t1 = (T)t.getClass().newInstance();
        for (Field field : fields) {
          field.setAccessible(true);
          Column column = field.getAnnotation(Column.class);
          Object value = resultSet.getObject(column.value());
          field.set(t1, value);
        }
        result.add(t1);
      }
    } catch (Exception e) {
      throw new HiveJdbcAuthorityException("authority query is failed:", e);
    } finally {
      AuthJdbc.cleanupResources(connection, statement, resultSet);
    }

    return result;
  }

}
