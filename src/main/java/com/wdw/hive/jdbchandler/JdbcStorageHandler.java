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

import com.wdw.hive.jdbchandler.conf.JdbcStorageConfigManager;
import com.wdw.hive.jdbchandler.utils.LogUtil;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;

public class JdbcStorageHandler implements HiveStorageHandler {

  //private final static Logger LOGGER = LoggerFactory.getLogger(JdbcStorageHandler.class);

  private static final LogUtil LOGGER = LogUtil.getLogger();

  private Configuration conf;

  public JdbcStorageHandler() {
    LOGGER.info("######JdbcStorageHandler######new JdbcStorageHandler");
    SessionState sess = SessionState.get();
    Configuration sessionConf = sess.getConf();
    String peKey = "hive.exec.post.hooks";
    String peVal = sessionConf.get(peKey);
    peVal = peVal == null? "": peVal;
    if (!peVal.contains(QJdbcPostExecuteHook.class.getName())) {
      String peValUp = QJdbcPostExecuteHook.class.getName() + "," + peVal;
      sessionConf.set(peKey, peValUp);
      LOGGER.info("######JdbcStorageHandler######" + peKey + "\t" + peVal + "\t" + peValUp);

      String localAutoKey = "hive.exec.mode.local.auto";
      String localAutoVal = sessionConf.get(localAutoKey);
      sess.getHiveVariables().put("hive.exec.mode.local.auto.prejdbc", localAutoVal);
      sessionConf.set(localAutoKey, "false");
      LOGGER.info("######JdbcStorageHandler######" + localAutoKey + "\t" + localAutoVal);
    }
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }


  @Override
  public Configuration getConf() {
    return this.conf;
  }


  @SuppressWarnings("rawtypes")
  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return JdbcInputFormat.class;
  }


  @SuppressWarnings("rawtypes")
  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return JdbcOutputFormat.class;
  }


  @Override
  public Class<? extends AbstractSerDe> getSerDeClass() {
    return JdbcSerDe.class;
  }


  @Override
  public HiveMetaHook getMetaHook() {
    return new JdbcHiveMetaHook();
  }


  @Override
  public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    Properties properties = tableDesc.getProperties();
    LOGGER.info("configureTableJobProperties:" + properties);
    //JdbcStorageConfigManager.copyConfigurationToJob(properties, jobProperties);
  }


  @Override
  public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    Properties properties = tableDesc.getProperties();
    LOGGER.info("configureInputJobProperties:" + properties);
    JdbcStorageConfigManager.copyConfigurationToJob(properties, jobProperties);
  }


  @Override
  public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    Properties properties = tableDesc.getProperties();
    LOGGER.info("configureOutputJobProperties:" + properties);
  }


  @Override
  public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException {
    return null;
  }

  @Override
  public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {

  }

}
