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

public class DoubleIntervalSplitter extends Splitter {

  @Override
  public void initSplitter(String lowerBound, String upperBound, String partitionColumn,
      Integer partitionNum, Configuration conf) throws HiveJdbcDatabaseAccessException {
    super.initSplitter(lowerBound, upperBound, partitionColumn, partitionNum, conf);
    double doubleLower = Double.parseDouble(lowerBound);
    double doubleUpper = Double.parseDouble(upperBound);
    double doubleInterval = (doubleUpper - doubleLower) / (double) this.getPartitionNum();
    long lenght = new Double(doubleInterval).longValue();
    this.setNullPartLength(lenght);
    double splitDoubleLower, splitDoubleUpper;
    for (int i = 0; i < this.getPartitionNum(); i++) {
      splitDoubleLower = doubleLower + doubleInterval * i;
      splitDoubleUpper = doubleLower + doubleInterval * (i + 1);
      if (splitDoubleUpper > splitDoubleLower) {
        this.addSplit(splitDoubleLower, splitDoubleUpper, lenght == 0 ? 1 : lenght, i);
      }
    }
  }

  @Override
  public String quotes() {
    return "";
  }

}
