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

import com.wdw.hive.jdbchandler.JdbcInputSplit;
import com.wdw.hive.jdbchandler.conf.JdbcStorageConfigManager;
import com.wdw.hive.jdbchandler.exception.HiveJdbcDatabaseAccessException;
import java.time.LocalDate;
import java.time.Period;
import org.apache.hadoop.conf.Configuration;

public class DateIntervalSplitter extends Splitter {

  @Override
  public void initSplitter(String lowerBound, String upperBound, String partitionColumn,
      Integer partitionNum, Configuration conf) throws HiveJdbcDatabaseAccessException {
    super.initSplitter(lowerBound, upperBound, partitionColumn, partitionNum, conf);

    LocalDate dateLower = LocalDate.parse(lowerBound);
    LocalDate dateUpper = LocalDate.parse(upperBound);
    Period between = Period.between(dateLower, dateUpper);
    int days = (int) (dateUpper.toEpochDay() - dateLower.toEpochDay() + 1);

    if (days <= this.getPartitionNum()) {
      this.setNullPartLength(Long.valueOf(1));
      for (int i = 0; i < days; i++) {
        this.getSplits().add(
            new JdbcInputSplit(getSingleQuery(conf, dateLower.plusDays(i).toString()), 0L, 1L,
                this.getPath()));
      }
      return;
    }
    int size = days / this.getPartitionNum();
    int remainder = days % this.getPartitionNum();

    this.setNullPartLength(Long.valueOf(size));

    for (int i = 0; i < this.getPartitionNum(); i++) {
      dateUpper = dateLower.plusDays(size);
      long lenght = size;
      if (remainder > 0) {
        dateUpper = dateUpper.plusDays(1);
        lenght += 1;
        remainder -= 1;
      }
      this.addSplit(dateLower, dateUpper, lenght, i);
      dateLower = dateUpper;
    }
  }

  private String getSingleQuery(Configuration conf, String date) {
    String sql = JdbcStorageConfigManager.getQueryToExecute(conf);
    return sql + (sql.toLowerCase().contains("where") ? " and " : " where ") + this
        .getPartitionColumn() + " = " + quotes() + date + quotes();
  }


}
