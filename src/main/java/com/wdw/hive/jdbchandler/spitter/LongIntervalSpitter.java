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
package com.wdw.hive.jdbchandler.spitter;

import com.wdw.hive.jdbchandler.exception.HiveJdbcDatabaseAccessException;
import org.apache.hadoop.conf.Configuration;

/**
 * @author root
 */
public class LongIntervalSpitter extends Splitter {

  public LongIntervalSpitter() {
  }


  @Override
  public void initSplitter(String lowerBound, String upperBound, String partitionColumn,
      Integer partitionNum, Configuration conf) throws HiveJdbcDatabaseAccessException {
    super.initSplitter(lowerBound, upperBound, partitionColumn, partitionNum, conf);
    long longLower = Long.parseLong(this.getLowerBound());
    long longUpper = Long.parseLong(this.getUpperBound());
    if ((longUpper - longLower) < this.getPartitionNum()) {
      throw new HiveJdbcDatabaseAccessException(
          "number type parttion nums must > (max(partionColumn) - min(partitionColumn))");
    }
    double longInterval = (longUpper - longLower + 1) / (double) this.getPartitionNum();
    long length = new Double(longInterval).longValue();
    this.setNullPartLength(length);
    long splitLongLower, splitLongUpper;
    for (int i = 0; i < this.getPartitionNum(); i++) {
      splitLongLower = Math.round(longLower + longInterval * i);
      splitLongUpper = Math.round(longLower + longInterval * (i + 1));
      if (splitLongUpper > splitLongLower) {
        this.addSplit(splitLongLower, splitLongUpper, length, i);
      }
    }
  }

  @Override
  public String quotes() {
    return "";
  }

}
