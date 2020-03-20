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
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import org.apache.hadoop.conf.Configuration;

public class DecimalIntervalSplitter extends Splitter {

  @Override
  public void initSplitter(String lowerBound, String upperBound, String partitionColumn,
      Integer partitionNum, Configuration conf) throws HiveJdbcDatabaseAccessException {
    super.initSplitter(lowerBound, upperBound, partitionColumn, partitionNum, conf);

    BigDecimal decimalLower = new BigDecimal(lowerBound);
    BigDecimal decimalUpper = new BigDecimal(upperBound);
    int scale = decimalUpper.scale();
    BigDecimal decimalInterval = (decimalUpper.subtract(decimalLower))
        .divide(new BigDecimal(this.getPartitionNum()), MathContext.DECIMAL64);
    BigDecimal splitDecimalLower, splitDecimalUpper;
    long length = decimalInterval.longValue();
    this.setNullPartLength(length);
    for (int i = 0; i < this.getPartitionNum(); i++) {
      splitDecimalLower = decimalLower.add(decimalInterval.multiply(new BigDecimal(i)))
          .setScale(scale, RoundingMode.HALF_EVEN);
      splitDecimalUpper = decimalLower.add(decimalInterval.multiply(new BigDecimal(i + 1)))
          .setScale(scale, RoundingMode.HALF_EVEN);
      if (splitDecimalLower.compareTo(splitDecimalUpper) < 0) {
        this.addSplit(splitDecimalLower, splitDecimalLower, length, i);
      }
    }
  }

  @Override
  public String quotes() {
    return "";
  }

}
