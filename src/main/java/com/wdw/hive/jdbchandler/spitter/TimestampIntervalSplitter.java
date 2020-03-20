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
import java.sql.Timestamp;
import org.apache.hadoop.conf.Configuration;

public class TimestampIntervalSplitter extends Splitter {


  @Override
  public void initSplitter(String lowerBound, String upperBound, String partitionColumn,
      Integer partitionNum, Configuration conf) throws HiveJdbcDatabaseAccessException {
    super.initSplitter(lowerBound, upperBound, partitionColumn, partitionNum, conf);

    Timestamp timestampLower = Timestamp.valueOf(lowerBound);
    Timestamp timestampUpper = Timestamp.valueOf(upperBound);

    double timestampInterval =
        (timestampUpper.getTime() - timestampLower.getTime()) / (double) this.getPartitionNum();
    long length = new Double(timestampInterval).longValue() / (3600 * 24 * 1000);
    this.setNullPartLength(length);
    Timestamp splitTimestampLower, splitTimestampUpper;
    for (int i = 0; i < this.getPartitionNum(); i++) {
      splitTimestampLower = new Timestamp(
          Math.round(timestampLower.getTime() + timestampInterval * i));
      splitTimestampUpper = new Timestamp(
          Math.round(timestampLower.getTime() + timestampInterval * (i + 1)));
      if (splitTimestampLower.compareTo(splitTimestampUpper) < 0) {
        this.addSplit(splitTimestampLower, splitTimestampUpper, length, i);
      }
    }
  }
}
