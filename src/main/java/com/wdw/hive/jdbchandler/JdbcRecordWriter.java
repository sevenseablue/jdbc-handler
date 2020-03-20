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

import com.wdw.hive.jdbchandler.conf.DatabaseType;
import com.wdw.hive.jdbchandler.conf.JdbcStorageConfig;
import com.wdw.hive.jdbchandler.dao.DatabaseAccessor;
import com.wdw.hive.jdbchandler.dao.DatabaseAccessorFactory;
import com.wdw.hive.jdbchandler.exception.HiveJdbcDatabaseAccessException;
import com.wdw.hive.jdbchandler.utils.Constant;
import com.wdw.hive.jdbchandler.utils.LogUtil;
import com.wdw.hive.jdbchandler.utils.Tuple2;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;

public class JdbcRecordWriter implements RecordWriter {

  //private static final Logger LOGGER = LoggerFactory.getLogger(JdbcRecordWriter.class);

  private static final LogUtil LOGGER = LogUtil.getLogger();
  DatabaseAccessor dbAccessor = null;
  private Connection conn = null;
  private PreparedStatement state = null;
  // 多少次执行
  private Integer counter = 0;
  private int batchsize;

  public JdbcRecordWriter(JobConf conf) throws HiveJdbcDatabaseAccessException {

    dbAccessor = DatabaseAccessorFactory.getAccessor(conf);
    LOGGER.info("dbtype:{}", JdbcStorageConfig.DATABASE_TYPE.getConfigValue(conf));
    DatabaseType dbType = DatabaseType
        .valueOf(JdbcStorageConfig.DATABASE_TYPE.getConfigValue(conf));
    switch (dbType) {
      default:
        break;
    }
    Tuple2<Connection, PreparedStatement> tup = dbAccessor.buildConnect(conf);
    this.conn = tup._1();
    this.state = tup._2();
    this.batchsize = JdbcStorageConfig.JDBC_BATCH_SIZE.getConfigIntValue(conf, Constant.BATCH_SIZE);
    LOGGER.info("batch insert size is : {}", this.batchsize);
  }

  @Override
  public void write(Writable w) throws IOException {
    if (w instanceof JdbcWritable) {
      JdbcWritable wr = (JdbcWritable) w;
      try {
        wr.write(state);
        state.addBatch();
        counter += 1;
        if (counter == batchsize) {
          state.executeBatch();
          conn.commit();
          counter = 0;
        }
      } catch (SQLException e) {
        LOGGER.error("record writer write failed", e);
        throw new IOException("record writer write failed");
      }
    }

  }

  @Override
  public void close(boolean abort) throws IOException {
    if (abort) {
      try {
        conn.rollback();
      } catch (SQLException ex) {
        LOGGER.warn(StringUtils.stringifyException(ex));
      } finally {
        try {
          conn.close();
        } catch (SQLException ex) {
          throw new IOException(ex.getMessage());
        }
      }
    } else {
      this.close();
    }
    dbAccessor.close();
  }

  public void close() throws IOException {
    try {
      state.executeBatch();
      conn.commit();
    } catch (SQLException e) {
      try {
        conn.rollback();
      } catch (SQLException ex) {
        LOGGER.warn(StringUtils.stringifyException(ex));
      }
      throw new IOException(e.getMessage());
    } finally {
      try {
        if (state != null) {
          state.close();
        }
        if (conn != null) {
          conn.close();
        }
      } catch (SQLException ex) {
        throw new IOException(ex.getMessage());
      }
    }
  }

}
