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

import com.wdw.hive.jdbchandler.authority.AuthorityManager;
import com.wdw.hive.jdbchandler.conf.JdbcStorageConfig;
import com.wdw.hive.jdbchandler.conf.JdbcStorageConfigManager;
import com.wdw.hive.jdbchandler.dao.DatabaseAccessor;
import com.wdw.hive.jdbchandler.dao.DatabaseAccessorFactory;
import com.wdw.hive.jdbchandler.exception.HiveJdbcAuthorityException;
import com.wdw.hive.jdbchandler.utils.Constant;
import com.wdw.hive.jdbchandler.utils.HiveJdbcBridgeUtils;
import com.wdw.hive.jdbchandler.utils.LogUtil;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class JdbcSerDe extends AbstractSerDe {

  //private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSerDe.class);

  private static final LogUtil LOGGER = LogUtil.getLogger();

  private StructObjectInspector objectInspector;
  private int numColumns;
  private String[] hiveColumnTypeArray;
  private List<String> columnNames;
  private List<Object> row;
  private JdbcWritable jdbcWritable;
  private PrimitiveTypeInfo[] hiveColumnTypes;

  private Set<Integer> indexs;

  /*
   * This method gets called multiple times by Hive. On some invocations, the properties will be empty.
   * We need to detect when the properties are not empty to initialise the class variables.
   *
   * @see org.apache.hadoop.hive.serde2.Deserializer#initialize(org.apache.hadoop.conf.Configuration, java.util.Properties)
   */
  @Override
  public void initialize(Configuration conf, Properties tbl) throws SerDeException {
    try {

      LOGGER.info("Initializing the SerDe");
      LOGGER.info("configuration:{}", conf);
      LOGGER.info("tableProperties:{}", tbl);

      // Hive cdh-4.3 does not provide the properties object on all calls
      if (tbl.containsKey(JdbcStorageConfig.DATABASE_TYPE.getPropertyName())) {
        Configuration tableConfig = JdbcStorageConfigManager.convertPropertiesToConfiguration(tbl);
        if(!tableConfig.get(JdbcStorageConfig.PASSWORD.getPropertyName()).equals(Constant.STAR6)) {
          //验证是否有权限，
          String power = null;
          try {
            power = AuthorityManager.getAuthority(tableConfig);
            LOGGER.info("power" + power);
          } catch (HiveJdbcAuthorityException e) {
            throw new SerDeException(e.getMessage());
          }
          if (StringUtils.isBlank(power)) {
            throw new SerDeException("no authority");
          }
          if (power.contains("insert")) {
            tableConfig.set(JdbcStorageConfig.USERACCOUNT.getPropertyName(), "rw");
          } else {
            tableConfig.set(JdbcStorageConfig.USERACCOUNT.getPropertyName(), "r");
          }
        }

        DatabaseAccessor dbAccessor = DatabaseAccessorFactory.getAccessor(tableConfig);
        columnNames = dbAccessor.getColumnNames(tableConfig);
        numColumns = columnNames.size();
        row = new ArrayList<Object>(numColumns);

        String[] hiveColumnNameArray = parseProperty(tbl.getProperty(serdeConstants.LIST_COLUMNS),
            ",");
        if (numColumns != hiveColumnNameArray.length) {
          throw new SerDeException("Expected " + numColumns + " columns. Table definition has "
              + hiveColumnNameArray.length + " columns");
        }
        List<String> hiveColumnNames = Arrays.asList(hiveColumnNameArray);

        hiveColumnTypeArray = parseProperty(tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES), ":");
        if (hiveColumnTypeArray.length == 0) {
          throw new SerDeException("Received an empty Hive column type definition");
        }

        hiveColumnTypes = new PrimitiveTypeInfo[numColumns];
        ArrayList<TypeInfo> typeInfosFromTypeString = TypeInfoUtils
            .getTypeInfosFromTypeString(tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES));

        List<ObjectInspector> fieldInspectors = new ArrayList<ObjectInspector>(numColumns);
        for (int i = 0; i < numColumns; i++) {
          hiveColumnTypes[i] = (PrimitiveTypeInfo) typeInfosFromTypeString.get(i);
          fieldInspectors.add(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
              TypeInfoFactory.getPrimitiveTypeInfo(hiveColumnTypeArray[i])));
        }
        objectInspector = ObjectInspectorFactory
            .getStandardStructObjectInspector(hiveColumnNames, fieldInspectors);

        //初始化jdbc writable 得到映射的java类
        indexs = JdbcStorageConfigManager.getWriteIgnoreIndex(tableConfig, columnNames);
        LOGGER.info("jdbc writable mapping ignore java type : {}", indexs);
        int[] types = HiveJdbcBridgeUtils.hiveTypesToSqlTypes(hiveColumnTypeArray);
        LOGGER.info("jdbc writable mapping origin java type : {}", types);
        int[] resTypes = JdbcStorageConfigManager
            .removeIgnoreTypes(tableConfig, columnNames, types);
        LOGGER.info("jdbc writable mapping java res type : {}", resTypes);
        this.jdbcWritable = new JdbcWritable(resTypes);
      }

//      tbl.put(JdbcStorageConfig.PASSWORD.getPropertyName(), Constant.STAR6);
    } catch (Exception e) {
      LOGGER.error("Caught exception while initializing the SqlSerDe", e);
      throw new SerDeException(e);
    }
  }


  private String[] parseProperty(String propertyValue, String delimiter) {
    if ((propertyValue == null) || (propertyValue.trim().isEmpty())) {
      return new String[]{};
    }
    return propertyValue.split(delimiter);
  }


  @Override
  public Object deserialize(Writable blob) throws SerDeException {
    if (!(blob instanceof MapWritable)) {
      throw new SerDeException("Expected MapWritable. Got " + blob.getClass().getName());
    }

    if ((row == null) || (columnNames == null)) {
      throw new SerDeException("JDBC SerDe hasn't been initialized properly");
    }

    row.clear();
    MapWritable input = (MapWritable) blob;
    Text columnKey = new Text();
    input.size();
    for (int i = 0; i < numColumns; i++) {
      columnKey.set(columnNames.get(i));
      Writable value = input.get(columnKey);
      Object obj;
      //LOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>value.getClass():{}",value.getClass());
      if (value instanceof NullWritable) {
        obj = null;
      } else {
        obj = ((ObjectWritable) value).get();
        //OGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>getPrimitiveCategory"+hiveColumnTypes[i].getPrimitiveCategory());
        switch (hiveColumnTypes[i].getPrimitiveCategory()) {
          case SHORT:
            if (obj instanceof Number) {
              obj = new ShortWritable(((Number) obj).shortValue());
            } else {
              obj = new ShortWritable(Short.valueOf(obj.toString()));
            }
            break;
          case INT:
            if (obj instanceof Number) {
              obj = new IntWritable(((Number) obj).intValue());
            } else {
              obj = new IntWritable(Integer.valueOf(obj.toString()));
            }
            break;
          case BYTE:
            if (obj instanceof Number) {
              obj = new ByteWritable(((Number) obj).byteValue());
            } else {
              obj = new ByteWritable(Byte.valueOf(obj.toString()));
            }
            break;
          case LONG:
            if (obj instanceof Long) {
              obj = new LongWritable(((Number) obj).longValue());
            } else {
              obj = new LongWritable(Long.valueOf(obj.toString()));
            }
            //LOGGER.info(">>>>>>LONG:{}",obj);
            break;
          case FLOAT:
            if (obj instanceof Number) {
              obj = new FloatWritable(((Number) obj).floatValue());
            } else {
              obj = new FloatWritable(Float.valueOf(obj.toString()));
            }
            //LOGGER.info(">>>>>>Float:{}",obj);
            break;
          case DOUBLE:
            if (obj instanceof Number) {
              obj = new DoubleWritable(((Number) obj).doubleValue());
            } else {
              obj = new DoubleWritable(Double.valueOf(obj.toString()));
            }
            //LOGGER.info(">>>>>>DOUBLE:{}",obj);
            break;
          case DECIMAL:
            int scale = ((DecimalTypeInfo) hiveColumnTypes[i]).getScale();
            obj = new HiveDecimalWritable(
                HiveDecimal.create(obj.toString()).setScale(scale, BigDecimal.ROUND_HALF_EVEN));
            //LOGGER.info(">>>>>>DECIMAL:{}",obj);
            break;
          case BOOLEAN:
            if (obj instanceof Number) {
              obj = new BooleanWritable(((Number) value).intValue() != 0);
            } else if (obj instanceof Boolean) {
              obj = new BooleanWritable((Boolean) obj);
            } else {
              obj = new BooleanWritable(Boolean.valueOf(value.toString()));
            }
            //LOGGER.info(">>>>>>BOOLEAN:{}",obj);
            break;
          case CHAR:
          case VARCHAR:
          case STRING:
            obj = new Text(obj.toString());
            //LOGGER.info(">>>>>>STRING:{}",obj);
            break;
          case DATE:
            obj = new DateWritable(Date.valueOf(obj.toString()));
            //LOGGER.info(">>>>>>Date:{}",obj);
            break;
          case TIMESTAMP:
            if (obj instanceof Timestamp) {
              obj = new TimestampWritable((Timestamp) obj);
            } else {
              obj = new TimestampWritable(Timestamp.valueOf(obj.toString()));
            }
            //LOGGER.info(">>>>>>TIMESTAMP:{}",obj);
            break;
          case BINARY:
            if (obj instanceof byte[]) {
              obj = new BytesWritable((byte[]) obj);
            } else {
              obj = new BytesWritable(obj.toString().getBytes());
            }
            //LOGGER.info(">>>>>>BINARY:{}",obj);
            break;
          default:
            throw new SerDeException("no find match type");
        }
      }
      row.add(obj);
    }
    return row;
  }


  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    LOGGER.info("get ObjectInspector");
    return objectInspector;
  }


  @Override
  public Class<? extends Writable> getSerializedClass() {
    return MapWritable.class;
  }


  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    StructObjectInspector structObjectInspector = (StructObjectInspector) objInspector;

    List<? extends StructField> fields = structObjectInspector.getAllStructFieldRefs();
    if (fields.size() != columnNames.size()) {
      throw new SerDeException(
          String.format("Required %d columns, received %d.", columnNames.size(), fields.size()));
    }
    jdbcWritable.clear();
    int index = 0;
    for (int i = 0; i < columnNames.size(); i++) {
      if (indexs.contains(i)) {
        continue;
      }
      StructField structField = fields.get(i);
      if (structField != null) {
        Object field = structObjectInspector.getStructFieldData(obj, structField);
        //LOGGER.info("obj class:{}",obj.getClass());
        ObjectInspector fieldOI = structField.getFieldObjectInspector();
        //LOGGER.info("fieldOI type:{}",fieldOI.getTypeName());
        Object javaObject = HiveJdbcBridgeUtils.deparseObject(field, fieldOI);
        //LOGGER.info("javaObject class:{}",javaObject.getClass());
        if (javaObject instanceof HiveDecimal) {
          HiveDecimal hiveDecimal = (HiveDecimal) javaObject;
          jdbcWritable.set(index, hiveDecimal.bigDecimalValue());
        } else {
          jdbcWritable.set(index, javaObject);
        }
        index += 1;
      }
    }
    return jdbcWritable;
  }


  @Override
  public SerDeStats getSerDeStats() {
    return null;
  }

}
