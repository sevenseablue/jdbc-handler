package com.wdw.hive.jdbchandler.model;


public class DeleteCol {
  String name;
  String delType;
  String val;

  public DeleteCol(String name, String delType, String val) {
    this.name = name;
    this.delType = delType;
    this.val = val;
  }

  public String getPrepare() {

    return null;
  }

  public String getDelType() {
    return delType;
  }

  public void setDelType(String delType) {
    this.delType = delType;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getVal() {
    return val;
  }

  public void setVal(String val) {
    this.val = val;
  }
}
