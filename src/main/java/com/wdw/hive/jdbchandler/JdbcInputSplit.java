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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;

public class JdbcInputSplit extends FileSplit implements InputSplit {

  private static final String[] EMPTY_ARRAY = new String[]{};

  private String query;
  private Long start;
  private Long length;

  public JdbcInputSplit() {
    super(null, 0, 0, EMPTY_ARRAY);
    this.start = -1L;
    this.length = 0L;
  }

  public JdbcInputSplit(Path dummyPath) {
    super(dummyPath, 0, 0, EMPTY_ARRAY);
    this.start = -1L;
    this.length = 0L;
  }

  public JdbcInputSplit(String query, long start, long lenght, Path dummyPath) {
    super(dummyPath, 0, 0, EMPTY_ARRAY);
    this.query = query;
    this.start = start;
    this.length = lenght;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeUTF(query);
    out.writeLong(start);
    out.writeLong(length);
  }


  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    query = in.readUTF();
    start = in.readLong();
    length = in.readLong();
  }

  public String getQuery() {
    return query;
  }

  @Override
  public long getLength() {
    return length;
  }

  public void setLength(Long length) {
    this.length = length;
  }

  @Override
  public String[] getLocations() throws IOException {
    return EMPTY_ARRAY;
  }

  @Override
  public String toString() {
    return "JdbcInputSplit{" +
        "query='" + query + '\'' +
        ", start=" + start +
        ", length=" + length +
        '}';
  }
}
