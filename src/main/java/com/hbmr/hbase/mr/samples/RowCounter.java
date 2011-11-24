package com.hbmr.hbase.mr.samples;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import com.hbmr.hbase.mr.MrAction;

/**
 * Sample mapreduce action: counts rows in a table.
 * TODO(vlad): ignore the rows marked as deleted
 *
 * @author Vlad Patryshev
 */
public class RowCounter extends MrAction {
  private static final String KEY = "counter.key";

  public Map<String, String> map(String rowkey, Result data, Configuration config) {
    String key = config.get(KEY);
    Map<String, String> result = new HashMap<String, String>();
    for (KeyValue value : data.list()) {
      if (value.getValue().length > 0) {
        result.put(key, "1");
        break;
      }
    }
    return result;
  }

  public String reduce(Iterable<String> values, Configuration config) {
    long sum = 0;
    for (String value : values) {
      try {
        sum += Long.parseLong(value);
      } catch (Exception e) {
        // just ignore bad strings
      }
    }
    return Long.toString(sum);
  }
}
