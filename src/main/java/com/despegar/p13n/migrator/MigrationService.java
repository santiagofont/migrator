package com.despegar.p13n.migrator;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MigrationService {

    protected static final Logger LOG = LoggerFactory.getLogger(MigrationService.class);
    
    public static final String HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    public static final String ZOOKEEPER_ZNODE_PARENT = "zookeeper.znode.parent";
    public static final String FS_DEFAULT_NAME = "fs.default.name";

    public static final String HBASE_CLIENT_RETRIES = "hbase.client.retries.number";
    public static final String MAPRED_JOB_TRACKER = "mapred.job.tracker";

    private static final String RUN_MODE = "run";
    private static final String TEST_MODE = "test";
    
    
    private String hbaseZkQuorum;

    private String hbaseZNodeParent;

    private String fsDefaultName;

    private String jobTracker;

    private String sourceZookeeperQuorum;

    private String sourceZNodeParent;

    private String sourceFsDefaultName;

    private int sourceRetries;
    
    private String jar;
    
    private String batchSize;
    
    private boolean synchronic;
    
    private Properties environmentProperties;
    private Properties tablesProperties;
    
    private Configuration conf;

	public static void main(String[] args) {
		
		if(args.length != 3) {
			wrongArguments();
		}
		String mode = args[2];
		if (!mode.equalsIgnoreCase(RUN_MODE) && !mode.equalsIgnoreCase(TEST_MODE)) {
			wrongArguments();
		}
		MigrationService instance = null;
		try {
			instance = new MigrationService(args[0], args[1]);
			instance.initializeHbase();
		} catch (IOException e) {
			LOG.error("Error loading configuration files:"+e.getMessage(), e);
			System.exit(1);
		} catch (URISyntaxException e) {
			LOG.error("Error configuring hbase source connection:"+e.getMessage(), e);
			System.exit(1);
		}
		try {
			if (mode.equalsIgnoreCase(RUN_MODE)) {
				instance.process();
			} else {
				LOG.info("connection test finished");
			}
			System.exit(0);
		} catch (Exception e) {
			LOG.error("Error while processing:"+e.getMessage(), e);
			System.exit(1);
		}
		
	}
	
	public MigrationService(String environmentPath, String tablesPath) throws IOException {
		FileInputStream environmentInputStream = new FileInputStream( new File(environmentPath));
		environmentProperties = new Properties();
		environmentProperties.load(environmentInputStream);
		
		this.sourceFsDefaultName = this.loadProperty("hdfs.default.name");
	    this.jar = this.sourceFsDefaultName + this.loadProperty("jar");
		this.hbaseZkQuorum = this.loadProperty("target.zk.quorum");
	    this.hbaseZNodeParent = this.loadProperty("target.znode.parent");
	    this.fsDefaultName = this.loadProperty("target.fs.name");
	    this.jobTracker = this.loadProperty("mapred.job.tracker");
	    this.sourceZookeeperQuorum = this.loadProperty("hbase.zookeeper.quorum");
	    this.sourceZNodeParent = this.loadProperty("zookeeper.znode.parent");
	    this.sourceFsDefaultName = this.loadProperty("hdfs.default.name");
	    this.batchSize = this.loadProperty("batch.size");
	    this.synchronic = Boolean.parseBoolean(this.loadProperty("synchronic"));
	    this.sourceRetries = Integer.parseInt(this.loadProperty("hbase.client.retries.number"));
	    
		FileInputStream tablesInputStream = new FileInputStream( new File(tablesPath));
		tablesProperties = new Properties();
		tablesProperties.load(tablesInputStream);
		
		LOG.info("The following tables will be migrated");
		for(Entry<Object, Object> table : this.tablesProperties.entrySet()) {
			String source = table.getKey().toString();
			String target = table.getValue().toString();
			LOG.info(source+"="+target);
		}
	}
	
	private void process() throws IOException, ClassNotFoundException, InterruptedException {
		LOG.info("process begins");
		for(Entry<Object, Object> table : this.tablesProperties.entrySet()) {
			String source = table.getKey().toString();
			String target = table.getValue().toString();
			migrateTable(source, target);
		}
		LOG.info("process completed");
	}

	private void migrateTable(String source, String target) throws IOException,
			InterruptedException, ClassNotFoundException {
		LOG.info("migrating from {} to {}", source, target);
		conf.set(MigrationMapper.TARGET_TABLE_NAME, target);
		
		Job job = new Job(conf);
		job.setJarByClass(MigrationService.class);
		job.setJobName("migrating:" + source);

		Scan scan = new Scan();
		scan.setAttribute("scan.attributes.table.name", Bytes.toBytes(source));

		TableMapReduceUtil
		    .initTableMapperJob(source, scan, MigrationMapper.class, NullWritable.class, NullWritable.class, job);

		job.setOutputFormatClass(NullOutputFormat.class);
		
		if (this.synchronic) {
		    job.waitForCompletion(true);
		    LOG.info("table completed");
		} else {
		    job.submit();
		    final int jobId = job.getJobID().getId();
		    LOG.info("Job {} with id {} submitted to job tracker {}", job.getJobName(), jobId, this.jobTracker);
		    LOG.info("Job traking url: {} ", job.getTrackingURL());
		}
	}
   
    private void initializeHbase() throws URISyntaxException {
        conf = new Configuration();
        // source H1
        conf.set(MigrationService.MAPRED_JOB_TRACKER, this.jobTracker);
        conf.set(MigrationService.HBASE_ZOOKEEPER_QUORUM, this.sourceZookeeperQuorum);
        conf.set(MigrationService.ZOOKEEPER_ZNODE_PARENT, this.sourceZNodeParent);
        conf.set(MigrationService.FS_DEFAULT_NAME, this.sourceFsDefaultName);
        conf.setInt(MigrationService.HBASE_CLIENT_RETRIES, this.sourceRetries);

        // target H2
        conf.set(MigrationMapper.JAR, jar);
        conf.set(MigrationMapper.BATCH_SIZE, batchSize);

        conf.set(MigrationMapper.HBASE_ZK_QUORUM, this.hbaseZkQuorum);
        conf.set(MigrationMapper.HBASE_ZNODE_PARENT, this.hbaseZNodeParent);
        conf.set(MigrationMapper.FS_DEFAULT_NAME, this.fsDefaultName);

        DistributedCache.addCacheFile(new URI(this.jar), conf);

    }
    
	private String loadProperty(String propertyName) {
		String value = this.environmentProperties.getProperty(propertyName);
		LOG.info(propertyName+"="+value);
		return value;
	}
	
	private static void wrongArguments() {
		LOG.warn("Error on args");
		LOG.warn("Usage:");
		LOG.warn("java -jar migrator.jar <environment.properties> <tables.properties> [run|test]");
		System.exit(1);
	}


}