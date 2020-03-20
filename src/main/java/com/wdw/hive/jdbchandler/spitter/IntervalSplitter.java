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

public interface IntervalSplitter {

  /*List<MutablePair<String, String>> getIntervals(String lowerBound, String upperBound, int numPartitions, TypeInfo typeInfo);

  default List<MutablePair<String, String>> getIntervals(String lowerBound, String upperBound, int numPartitions) {
	return getIntervals(lowerBound,upperBound,numPartitions,null);
  }*/

  void initSplitter(String lowerBound, String upperBound, String partitionColumn,
      Integer partitionNum, Configuration conf) throws HiveJdbcDatabaseAccessException;

  String getQuery(String lowerBound, String upperBound, Configuration conf);

  String quotes();

}