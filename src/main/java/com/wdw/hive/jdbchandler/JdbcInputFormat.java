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

package com.wdw.hive.jdbchandler;

import com.wdw.hive.jdbchandler.conf.JdbcStorageConfig;
import com.wdw.hive.jdbchandler.dao.DatabaseAccessor;
import com.wdw.hive.jdbchandler.dao.DatabaseAccessorFactory;
import com.wdw.hive.jdbchandler.spitter.IntervalSplitterFactory;
import com.wdw.hive.jdbchandler.spitter.Splitter;
import com.wdw.hive.jdbchandler.utils.Constant;
import com.wdw.hive.jdbchandler.utils.LogUtil;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.math.BigDecimal;

public class JdbcInputFormat extends HiveInputFormat<LongWritable, MapWritable> {

  //private static final Logger LOGGER = LoggerFactory.getLogger(JdbcInputFormat.class);

  private static final LogUtil LOGGER = LogUtil.getLogger();
  private DatabaseAccessor dbAccessor = null;


  /**
   * {@inheritDoc}
   */
  @Override
  public RecordReader<LongWritable, MapWritable>
  getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {

    LOGGER.info("DATABASE_TYPE:{}", JdbcStorageConfig.DATABASE_TYPE.getConfigValue(job));
    LOGGER.info("TABLE:{}", JdbcStorageConfig.TABLE.getConfigValue(job));
    if (!(split instanceof JdbcInputSplit)) {
      throw new RuntimeException("Incompatible split type " + split.getClass().getName() + ".");
    }

    return new JdbcRecordReader(job, (JdbcInputSplit) split);
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {

    Splitter splitter;
    try {

      dbAccessor = DatabaseAccessorFactory.getAccessor(job);

      LOGGER.info("QUERY:{}", JdbcStorageConfig.QUERY.getConfigValue(job));
      LOGGER.info("JDBC_DRIVER_CLASS:{}", JdbcStorageConfig.JDBC_DRIVER_CLASS.getConfigValue(job));
      LOGGER.info("DATABASE_TYPE:{}", JdbcStorageConfig.DATABASE_TYPE.getConfigValue(job));
      LOGGER.info("TABLE:{}", JdbcStorageConfig.TABLE.getConfigValue(job));

      String partTbNamesStr = JdbcStorageConfig.PART_TABLES.getConfigValue(job, "");
      if(partTbNamesStr.contains(",") || partTbNamesStr.contains("[") || partTbNamesStr.contains("/")){
        return Splitter.initPartTbs(job, dbAccessor);
      }

      String partitionColumn = JdbcStorageConfig.PARTITION_COLUMN.getConfigValue(job);
      int type = 0;
      int partitionNum = JdbcStorageConfig.PARTITION_NUMS.getConfigIntValue(job, -1);
      int numRecords = dbAccessor.getTotalNumberOfRecords(job);
      if (partitionNum == -1) {
        partitionNum = (int) Math.round(new BigDecimal(
            (float) numRecords / JdbcStorageConfig.AUTO_SPLIT_SIZE
                .getConfigIntValue(job, Constant.DEFAULT_SPLIT_SIZE))
            .setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue());
        //partitionNum = numRecords / JdbcStorageConfig.AUTO_SPLIT_SIZE.getConfigIntValue(job, Constant.DEFAULT_SPLIT_SIZE);
      }
      String lowerBound = null;
      String upperBound = null;

      Pair<String, Integer> pairColType = dbAccessor.convertPartitionColumn(partitionColumn, job);
      if (numRecords < Constant.DEFAULT_SPLIT_SIZE || partitionNum <= 1 || pairColType == null) {
        LOGGER.info("numRecords:{},partition nums:{},pairColType:{} ##### " +
            "num records < 100W or partitionNum <= 1 or pairColType is null, " +
            "default one map to exec", numRecords, partitionNum, pairColType);
        InputSplit[] inputSplits = Splitter.initSingle(job, numRecords);
        LOGGER.info("default single partititon : {}", inputSplits[0].toString());
        return inputSplits;
      }

      // 参数处理
      partitionColumn = pairColType.getLeft();
      type = pairColType.getRight();
      Pair<String, String> boundary = dbAccessor.getBounds(job, partitionColumn);
      lowerBound = boundary.getLeft();
      upperBound = boundary.getRight();
      LOGGER.info(
          "spliter param:column:{},type:{},numRecord:{},partitionnums:{}, lowerBound:{},upperBound:{}",
          partitionColumn, type, numRecords, partitionNum, lowerBound, upperBound);

      // splitter
      splitter = IntervalSplitterFactory.newIntervalSpitter(type);
      splitter.initSplitter(lowerBound, upperBound, partitionColumn, partitionNum, job);
      LOGGER.info("splitter : [{}]", splitter.toString());
      InputSplit[] inputSplits = splitter.getSplits()
          .toArray(new InputSplit[splitter.getSplits().size()]);
      for (InputSplit inputSplit : inputSplits) {
        LOGGER.info("input spllits:[{}]", inputSplit.toString());
      }
      return inputSplits;

    } catch (Exception e) {
      LOGGER.error("Error while splitting input data.", e);
      throw new IOException(e);
    }
  }


  /**
   * For testing purposes only
   *
   * @param dbAccessor DatabaseAccessor object
   */
  public void setDbAccessor(DatabaseAccessor dbAccessor) {
    this.dbAccessor = dbAccessor;
  }

}
