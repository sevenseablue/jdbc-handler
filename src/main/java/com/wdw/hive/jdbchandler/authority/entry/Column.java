package com.wdw.hive.jdbchandler.authority.entry;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Created with Lee. Date: 2019/9/25 Time: 14:45 To change this template use File | Settings | File
 * Templates. Description:
 *
 * @author :
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Column {

  String value() default "";
}
