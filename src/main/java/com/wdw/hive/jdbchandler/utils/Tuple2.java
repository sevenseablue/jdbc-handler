package com.wdw.hive.jdbchandler.utils;

/**
 * Created with Lee. Date: 2019/8/28 Time: 17:43 To change this template use File | Settings | File
 * Templates. Description:
 *
 * @author :
 */
public class Tuple2<E, T> {

  private E e;

  private T t;

  public Tuple2(E e, T t) {
    this.e = e;
    this.t = t;
  }

  public E _1() {
    return this.e;
  }

  public T _2() {
    return this.t;
  }

  @Override
  public String toString() {
    return "<" + e.toString() + "," + t.toString() + ">";
  }

  @Override
  public boolean equals(Object obj) {
    if (obj != null) {
      Tuple2<E, T> other = (Tuple2<E, T>) obj;
      return e.equals(other.e) && t.equals(other.t);
    }
    return false;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 7;
    result = prime * result + ((e == null) ? 0 : e.hashCode());
    result = prime * result + ((t == null) ? 0 : t.hashCode());
    return result;
  }

}
