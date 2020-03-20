package com.wdw.hive.jdbchandler.utils;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

;import java.io.IOException;

public class HdfsUtil {
  static String HADOOP_HOME = System.getenv("HADOOP_HOME");
  public static FileSystem getFs(Configuration conf){
    try {
      if (HostUtil.isLocal()) {
        String HADOOP_HOME = System.getenv("HADOOP_HOME");
        conf.addResource(new Path("file://", HADOOP_HOME + "/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("file://", HADOOP_HOME + "/etc/hadoop/hdfs-site.xml"));
        conf.addResource(new Path("file://", HADOOP_HOME + "/etc/hadoop/mountTable.xml"));
      }
      return FileSystem.get(conf);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  public static void write(Configuration conf, String fileName, String str){
    FileSystem fs = getFs(conf);
    Path hdfsWritePath = new Path(fileName);
    FSDataOutputStream outputStream= null;
    try {
      outputStream = fs.create(hdfsWritePath, true);
      outputStream.writeBytes(str);
      outputStream.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static String read(Configuration conf, String fileName){
    FileSystem fs = getFs(conf);
    Path hdfsReadPath = new Path(fileName);
    FSDataInputStream inputStream = null;
    String out = null;
    try {
      inputStream = fs.open(hdfsReadPath);
      out = IOUtils.toString(inputStream, "UTF-8");
      inputStream.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return out;
  }

  public static void main(String[] args) {
    Configuration conf = new Configuration();
    String fileName = "/user/corphive/hive/conf/corphive/t1.txt";
    HdfsUtil.write(conf, fileName, "a\nb");
    System.out.println(HdfsUtil.read(conf, fileName));
  }
}
