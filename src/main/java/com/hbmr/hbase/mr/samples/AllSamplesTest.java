package com.hbmr.hbase.mr.samples;

import static java.util.Collections.*;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import com.hbmr.hbase.mr.Framework;
import com.hbmr.hbase.mr.MrJob;
import com.hbmr.hbase.mr.ParallelJob;
import com.hbmr.hbase.mr.SequentialJob;
import com.google.common.collect.Lists;

public class AllSamplesTest extends TestCase {
  private static final String COUNTER_KEY = "counter.key";
  private static final String FAMILY_NAME = "stats";
  static final String TABLE_NAME = "Stats..";

  @Before
  public void setUp() throws Exception {
    Framework framework = new Framework();
    framework.createTable("Pet", "family");
    framework.createTable("Person", "family");
  }

  @Test
  public void test_count_pets() throws Exception {
    Framework sut = new Framework();
    sut.createTable(TABLE_NAME, FAMILY_NAME);
    sut.set(COUNTER_KEY, "Pet");
    Scan scan = new Scan();
    scan.setFilter(new FirstKeyOnlyFilter());
    scan.addFamily(Bytes.toBytes("family"));
    MrJob job = sut.newJob("Counting Pets", "Pet", scan, "Stats..:stats:total", RowCounter.class);
    runAndCheck(job);
  }

  public static class PersonUpdates extends Update {

    @Override
    public List<? extends Row> transform(String rowkey, Result inputData, Configuration config) {
      try {
        String lastname = new String(inputData.getValue("family".getBytes(), "lastname".getBytes()), "UTF8");
        if (lastname.endsWith("sky")) {
          lastname = lastname.substring(0, lastname.length() - 3);
        } else {
          lastname += (lastname.endsWith("s") ? "ky" : "sky");
        }
        System.out.println("found " + rowkey + "->" + lastname);
        Put put = new Put(rowkey.getBytes());
        put.add("family".getBytes(), "lastname".getBytes(), lastname.getBytes());
        return singletonList(put);
      } catch (UnsupportedEncodingException e) {
        return Collections.emptyList();
      }
    }

  }

  @Test
  public void test_just_updates() throws Exception {
    Framework sut = new Framework();
    sut.createTable(TABLE_NAME, FAMILY_NAME);
    Scan scan = new Scan();
    scan.setFilter(new SingleColumnValueFilter(Bytes.toBytes("family"), Bytes.toBytes("gender"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("M")));
    scan.addFamily(Bytes.toBytes("family"));
    MrJob job = sut.newJobNoReduce("Scanning Persons", "Person", scan, "Person:family", PersonUpdates.class);
    runAndCheck(job);
  }

  private void runAndCheck(MrJob job) throws Exception {
    job.start();
    boolean response = job.waitForCompletion();
    assertTrue(response);
  }

  @Test
  public void test_count_all_tables() throws Exception {
    Framework sut = new Framework();
    HBaseAdmin admin = sut.hbaseAdmin();
    sut.createTable(TABLE_NAME, FAMILY_NAME);
    HTableDescriptor[] descriptors = admin.listTables();
    List<MrJob> jobs = new ArrayList<MrJob>();
    for (HTableDescriptor d : descriptors) {
      String tableName = d.getNameAsString();
      if (!tableName.contains("..")) {
        for (HColumnDescriptor c : d.getFamilies()) {
          sut.set(COUNTER_KEY, tableName);
          try {
            Scan scan = new Scan();
            scan.setFilter(new FirstKeyOnlyFilter());
            scan.addFamily(c.getName());
            MrJob job = sut.newJob("Counting " + tableName, tableName, scan, "Stats..:stats:total", RowCounter.class);
            jobs.add(job);
            break;
          } catch (Exception e) {
            System.out.println("failed on " + tableName);
            e.printStackTrace();
          }
        }
      }
    }

    SequentialJob sequence = new SequentialJob();
    for (List<MrJob> group : Lists.partition(jobs, 6)) {
      sequence.add(new ParallelJob(group));
    }

    boolean result = sequence.waitForCompletion();
    assertTrue("Failed... " + sequence.explainError(), result);
  }

}
