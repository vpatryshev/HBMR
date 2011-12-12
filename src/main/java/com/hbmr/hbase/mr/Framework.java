package com.hbmr.hbase.mr;

import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Map;

import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
/**
 * Mapreduce framework. Work in progress, will be documented later on.
 *
 * @author Vlad Patryshev
 */
@SuppressWarnings({"deprecation"})
public class Framework {
  public static final String ACTION_CLASS_KEY = "mr.framework.action.class";
  public static final String COUNTER_KEY = "counter.key";
  public static final String OUTPUT_COLUMN = "mr.framework.output";
  final static boolean LOCAL_HBASE = true;
  final static String WHERE_HBASE = LOCAL_HBASE ? "localhost" : "undefined";

  private Configuration config;

  public Framework(String... extraParams) {
    configure(extraParams);
  }

  public void createTable(String tableName, String... families) throws IOException {
    HBaseAdmin admin = hbaseAdmin();
    try {
      admin.getTableDescriptor(tableName.getBytes());
    } catch (TableNotFoundException tnf) {
      HTableDescriptor desc = new HTableDescriptor(tableName);
      for (String family: families) {
        desc.addFamily(new HColumnDescriptor(family));
      }
      admin.createTable(desc);
      admin.enableTable(tableName);
    }
  }

  private void configure(String... extraParams) {
    config = HBaseConfiguration.create();
    config.set("hbase.zookeeper.quorum", WHERE_HBASE);
    config.set("hbase.zookeeper.property.clientPort", "2181");
    try {
      new GenericOptionsParser(config, extraParams);
    } catch (Exception ioe) {
      ioe.printStackTrace();
    }
    config.set("mapred.output.committer.class",
        HBaseCommitter.class.getName());
  }

  /**
   * Provides mapreduce action for delegating mapper and reducer
   * @param configuration
   * @return a new mapreduce action
   * @throws ClassNotFoundException
   */
  static MrAction mrAction(Configuration configuration)
      throws InstantiationException, IllegalAccessException,
      ClassNotFoundException {
    return (MrAction) Class.forName(configuration.get(ACTION_CLASS_KEY)).newInstance();
  }

  public static class DelegatingMapper extends TableMapper<Writable, Writable> {

    private MrAction action;

    @Override
    protected void setup(Mapper<ImmutableBytesWritable, Result, Writable, Writable>.Context context) throws IOException,
        InterruptedException {
      try {
        Configuration configuration = context.getConfiguration();
        action = mrAction(configuration);
        Preconditions.checkNotNull(action);
      } catch (Exception e) {
        throw new IOException("Failed to instantiate MrAction: " + e);
      }
    }

    @Override
    public void map(ImmutableBytesWritable row, Result values,
                    Mapper<ImmutableBytesWritable, Result, Writable, Writable>.Context context) throws IOException, InterruptedException {
      Map<String, String> map = action.map(asString(row), values, context);
      for (String key : map.keySet()) {
        context.write(
            newIbw(key),
            newIbw(map.get(key)));
      }
    }
  }

  static class DelegatingReducer extends TableReducer<ImmutableBytesWritable, Result, Writable> {

    private MrAction action;
    private byte[] familyName;
    private byte[] columnName;
    Function<Object, String> toString = new Function<Object, String>() {
      @Override
      public String apply(Object input) {
        return input instanceof Result ? new String(((Result) input).getValue(familyName, columnName)) :
               input instanceof ImmutableBytesWritable ? asString((ImmutableBytesWritable) input) : input.toString();
      }
    };

    @Override
    protected void setup(Reducer<ImmutableBytesWritable, Result, Writable, Writable>.Context context) throws IOException, InterruptedException {
      try {
        Configuration configuration = context.getConfiguration();
        String[] col = configuration.get(OUTPUT_COLUMN).split(":");
        familyName = col[0].getBytes();
        columnName = col[1].getBytes();
        action = mrAction(configuration);
      } catch (Exception e) {
        throw new IOException("Failed to instantiate MrAction", e);
      }
    }

    private Put put(String row, Object value) {
      Put put = new Put(row.getBytes());
      put.add(familyName, columnName, value.toString().getBytes());
      return put;
    }

    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Result> values, Context context) throws IOException, InterruptedException {
      Preconditions.checkNotNull(values);
      Preconditions.checkNotNull(action);
      Iterable<String> valuesToReduce =
          values == null ? Collections.<String>emptyList() :
                           Iterables.transform(
                               Iterables.filter(values, Predicates.<Object>notNull()),
                               toString);

      String reduced = action.reduce(valuesToReduce, context == null ? null : context.getConfiguration());
      if (reduced != null) {
        context.write(key, put(Framework.asString(key), reduced));
      }
    }
  }

  private Job buildJob(String name, Class<? extends MrAction> mrClass) throws IOException {
    Job job = new Job(config, name);
    Configuration config = job.getConfiguration();
    ((JobConf) config).setJar(findMyJar());
    config.set(ACTION_CLASS_KEY, mrClass.getName());
    return job;
  }

  public MrJob newJob(String name, String dataTableName, String dataFamilyName, String outputPath, Class<? extends MrAction> mrClass)
      throws IOException {
    Scan scan = new Scan();
    scan.addFamily(Bytes.toBytes(dataFamilyName));
    return newJob(name, dataTableName, scan, outputPath, mrClass);
  }

  public MrJob newJob(String name, String dataTableName, Scan scan, String outputPath, Class<? extends MrAction> mrClass)
      throws IOException {
    Job job = buildJob(name, mrClass);
    mapJob(dataTableName, scan, job);
    initOutput(job, HBaseOutputFormat.class, outputPath);
    return new SimpleJob(job);
  }

  public MrJob newJobNoReduce(String name, String dataTableName, Scan scan, String outputPath, Class<? extends MrAction> mrClass)
      throws IOException {
    Job job = buildJob(name, mrClass);
    mapJob(dataTableName, scan, job);
    initOutput(job, HBaseOutputFormat.class, outputPath);
    return new SimpleJob(noReduction(job));
  }

  public MrJob newJobNoReduce(String name, String dataTableName, Scan scan, Class<? extends MrAction> mrClass)
      throws IOException {
    Job job = mapJob(dataTableName, scan, buildJob(name, mrClass));
    return new SimpleJob(noReduction(job));
  }

  private Job noReduction(Job job) {
    ((JobConf) job.getConfiguration()).setNumReduceTasks(0);
    return job;
  }

  private Job initOutput(Job job, Class<? extends OutputFormat> outputFormatClass, String outputPath) throws IOException {
    String[] resultNames = outputPath.split(":", 2);
    if (resultNames.length != 2) {
      throw new IllegalArgumentException("Result path should be like table:family:column, but I got \"" + outputPath + "\"");
    }
    TableMapReduceUtil.initTableReducerJob(resultNames[0], DelegatingReducer.class, job);

    Configuration c = job.getConfiguration();
    c.set(OUTPUT_COLUMN, resultNames[1]);
    c.set(TableOutputFormat.OUTPUT_TABLE, resultNames[0]);
    job.setOutputFormatClass(HBaseOutputFormat.class);
    return job;
  }

  private Job mapJob(String dataTableName, Scan scan, Job job)
      throws IOException {
    TableMapReduceUtil.initTableMapperJob(dataTableName, scan,
        DelegatingMapper.class, ImmutableBytesWritable.class,
        ImmutableBytesWritable.class, job);
    job.setOutputFormatClass(NoOutput.class);
    return job;
  }

  static ImmutableBytesWritable newIbw(String string) {
    return new ImmutableBytesWritable(string.getBytes());
  }

  static String asString(ImmutableBytesWritable row) {
    return new String(row.copyBytes());
  }

  static final Function<ImmutableBytesWritable, String> BYTES_TO_STRING = new Function<ImmutableBytesWritable, String>() {

    public String apply(ImmutableBytesWritable ibw) {
      return asString(ibw);
    }

  };

  private String findMyJar() {
    ClassLoader loader = getClass().getClassLoader();
    String class_file = getClass().getName().replaceAll("\\.", "/")
        + ".class";
    try {
      for (Enumeration itr = loader.getResources(class_file); itr
          .hasMoreElements(); ) {
        URL url = (URL) itr.nextElement();
        String path = url.getPath();
        if (path.startsWith("file:")) {
          path = path.substring("file:".length());
        }
        path = path.replaceAll("\\+", "%2B");
        path = URLDecoder.decode(path, "UTF-8");
        if ("jar".equals(url.getProtocol())) {
          return path.replaceAll("!.*$", "");
        } else if (path.contains("/target/")) {
          return path.substring(0, path.indexOf("/target/"))
              + "/target/hbmr-1.0.jar";
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return null;
  }

  public HBaseAdmin hbaseAdmin() throws MasterNotRunningException,
      ZooKeeperConnectionException {
    return new HBaseAdmin(config);
  }

  // TODO(vlad): get rid of this parasitic di asap
  public void set(String key, String value) {
    config.set(key, value);

  }

}
