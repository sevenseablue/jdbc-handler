/*
 * Copyright 2013-2015 Qubole
 * Copyright 2013-2015 Makoto YUI
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

package com.wdw.hive.jdbchandler;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import org.apache.hadoop.io.Writable;

public class JdbcWritable implements Writable {


  private Object[] columnValues;
  private int[] columnTypes;

  public JdbcWritable() {
  }

  public JdbcWritable(int[] types) {
    this.columnValues = new Object[types.length];
    this.columnTypes = types;
  }

  public void clear() {
    Arrays.fill(columnValues, null);
  }

  public void set(int i, Object javaObject) {
    columnValues[i] = javaObject;
  }

  public Object get(int i) {
    return columnValues[i];
  }


 /* public void readFields(ResultSet rs) throws SQLException {
  final ResultSetMetaData meta = rs.getMetaData();
	final int cols = meta.getColumnCount();
	final Object[] columns = new Object[cols];
	final int[] types = new int[cols];
	for (int i = 0; i < cols; i++) {
	  Object col = rs.getObject(i + 1);
	  columns[i] = col;
	  if (col == null) {
		types[i] = meta.getColumnType(i + 1);
	  }
	}
	this.columnValues = columns;
	this.columnTypes = types;
  }*/


  public void write(PreparedStatement statement) throws SQLException {
    assert (columnValues != null);
    assert (columnTypes != null);
    final Object[] r = this.columnValues;
    final int cols = r.length;
    if (statement == null) {
      throw new SQLException("stat is null");
    }
    for (int i = 0; i < cols; i++) {
      final Object col = r[i];
      if (col == null) {
        statement.setNull(i + 1, columnTypes[i]);
      } else {
        statement.setObject(i + 1, col, columnTypes[i]);
      }
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    throw new IOException("not should go here : [write]");
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    throw new IOException("not should go here : [readFields]");
  }
}
