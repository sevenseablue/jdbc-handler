package com.wdw.hive.jdbchandler.authority.entry;

import org.apache.commons.lang.StringUtils;

/**
 * Created with Lee. Date: 2019/9/25 Time: 14:36 To change this template use File | Settings | File
 * Templates. Description:
 *
 * @author :
 */
@Table("authority_info")
public class AuthorityInfo {

  @Column("id")
  private Integer id;
  @Column("username")
  private String username;
  @Column("password")
  private String passsword;
  @Column("ip")
  private String ip;
  @Column("host")
  private String host;
  @Column("port")
  private String port;
  @Column("namespace")
  private String namespace;
  @Column("database_name")
  private String database;
  @Column("table_name")
  private String tableName;
  @Column("power")
  private String power;

  public AuthorityInfo() {
  }

  public AuthorityInfo(String username, String passsword, String ip, String port, String database,
      String tableName) {
    if (StringUtils.isBlank(username) || StringUtils.isBlank(passsword) || StringUtils.isBlank(ip)
        || StringUtils.isBlank(port) ||
        StringUtils.isBlank(database) || StringUtils.isBlank(tableName)) {
      throw new RuntimeException("AuthorityInfo init failed");
    }
    this.username = username;
    this.passsword = passsword;
    this.ip = ip;
    this.port = port;
    this.database = database;
    this.tableName = tableName;
  }

  public AuthorityInfo(String username, String passsword, String namespace, String database,
      String tableName) {
    if (StringUtils.isBlank(username) || StringUtils.isBlank(passsword) || StringUtils
        .isBlank(namespace) || StringUtils.isBlank(database) || StringUtils.isBlank(tableName)) {
      throw new RuntimeException("AuthorityInfo init failed");
    }
    this.username = username;
    this.passsword = passsword;
    this.namespace = namespace;
    this.database = database;
    this.tableName = tableName;
  }

  public AuthorityInfo(String username, String namespace, String database,
                       String tableName) {
    if (StringUtils.isBlank(username) || StringUtils
        .isBlank(namespace) || StringUtils.isBlank(database) || StringUtils.isBlank(tableName)) {
      throw new RuntimeException("AuthorityInfo init failed");
    }
    this.username = username;
    this.namespace = namespace;
    this.database = database;
    this.tableName = tableName;
  }

  public Integer getId() {
    return id;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPasssword() {
    return passsword;
  }

  public void setPasssword(String passsword) {
    this.passsword = passsword;
  }

  public String getIp() {
    return ip;
  }

  public void setIp(String ip) {
    this.ip = ip;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public String getPort() {
    return port;
  }

  public void setPort(String port) {
    this.port = port;
  }

  public String getNamespace() {
    return namespace;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getPower() {
    return power;
  }

  public void setPower(String power) {
    this.power = power;
  }

  @Override
  public String toString() {
    return "AuthorityInfo{" +
        "id=" + id +
        ", username='" + username + '\'' +
        ", passsword='" + passsword + '\'' +
        ", ip='" + ip + '\'' +
        ", host='" + host + '\'' +
        ", port='" + port + '\'' +
        ", namespace='" + namespace + '\'' +
        ", database='" + database + '\'' +
        ", tableName='" + tableName + '\'' +
        ", power='" + power + '\'' +
        '}';
  }
}
