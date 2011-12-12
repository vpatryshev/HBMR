package com.hbmr.hbase.mr;

import static java.util.Collections.*;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;

import com.hbmr.hbase.mr.samples.RowCounter;
import com.hbmr.hbase.mr.samples.Update;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
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
    createPets(framework);
    createPeople(framework);
  }

  private void createPeople(Framework framework) throws IOException {
    framework.createTable("Person", "family");
    HTable table = new HTable("Person");
    add(table, "buddha", "name", "Gautama", "quote", "Hatred does not cease by hatred, but only by love; this is the eternal rule.");
    add(table, "obama", "name", "Barack Hussein Obama", "quote", "We need small change");
    add(table, "poe", "name", "Edgar Allan Poe", "quote", "tis some visitor entreating");
    add(table, "jesus", "name", "Jesus Christ", "quote", "Here is a true Israelite, in whom there is nothing false.");
    add(table, "vital", "name", "Vitaly Solomakha", "quote", "I cannot do two things in one day");
    add(table, "julien", "name", "Julien Wetterwald", "quote", "I know everything about temporal logic");
    add(table, "bozo", "name", "Boz Elloy", "quote", "You are all numbers to me");
    add(table, "lenin", "name", "Vladimir Ulyanov", "quote", "The Marxist doctrine is omnipotent because it is true.");
    add(table, "zarathustra", "name", "Zarathustra", "quote", "Hunger attacketh me");
    add(table, "sandusky", "name", "Sandusky", "quote", "I like young people");
    add(table, "", "name", "", "quote", "");
    add(table, "", "name", "", "quote", "");
    add(table, "", "name", "", "quote", "");
  }

  private void createPets(Framework framework) throws IOException {
    framework.createTable("Pet", "family");
    HTable table = new HTable("Pet");
    add(table, "basilio", "kind", "cat", "name", "Basilio");
    add(table, "matroskin", "kind", "cat", "name", "Matroskin");
    add(table, "hatulmadan", "kind", "cat", "name", "Hatul Madan");
    add(table, "puss", "kind", "cat", "name", "Puss'n'Boot");
    add(table, "neko", "kind", "cat", "name", "Neko-san");
    add(table, "ampersand", "kind", "cat", "name", "&");
    add(table, "cats", "kind", "cat", "name", "Category of All Cats");
    add(table, "sharik", "kind", "dog", "name", "Sharik");
    add(table, "aly", "kind", "dog", "name", "Aly");
    add(table, "mukhtar", "kind", "dog", "name", "Mukhtar");
    add(table, "dingo", "kind", "dog", "name", "Dingo the Wild");
    add(table, "lassie", "kind", "dog", "name", "Lassie");
    add(table, "methlab", "kind", "dog", "name", "Meth Lab");
    add(table, "sharik", "kind", "dog", "name", "Sharik");
    add(table, "fido", "kind", "dog", "name", "Fido");
    add(table, "at", "kind", "dog", "name", "@");
    add(table, "barbos", "kind", "dog", "name", "Barbos");
    add(table, "artemon", "kind", "dog", "name", "Artemon");
  }

  private void add(HTable table, String key, String... data) throws IOException {
    Put put = new Put(key.getBytes());
    for (int i = 0; i < data.length - 1; i += 2) {
      put.add("family".getBytes(), data[i].getBytes(), data[i+1].getBytes());
    }
    table.put(put);
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
        String name = new String(inputData.getValue("family".getBytes(), "name".getBytes()), "UTF8");
        if (name.endsWith("sky")) {
          name = name.substring(0, name.length() - 3);
        } else {
          name += (name.endsWith("s") ? "ky" : "sky");
        }
        System.out.println("found " + rowkey + "->" + name);
        Put put = new Put(rowkey.getBytes());
        put.add("family".getBytes(), "name".getBytes(), name.getBytes());
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
    assertTrue("Expected to succeed", response);
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
