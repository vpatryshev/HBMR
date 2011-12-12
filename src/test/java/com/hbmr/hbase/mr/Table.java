package com.hbmr.hbase.mr;

import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.client.HTable;

import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: vlad
 * Date: 11/29/11
 * Time: 8:37 AM
 * To change this template use File | Settings | File Templates.
 */
public class Table {
  HTable table;
  String name;
  Set<Family> families = Sets.newHashSet();
}
