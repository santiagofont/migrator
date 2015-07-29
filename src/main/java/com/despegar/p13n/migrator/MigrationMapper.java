package com.despegar.p13n.migrator;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.despegar.p13n.migrator.Consumer;
import com.google.common.collect.Lists;

public class MigrationMapper
    extends TableMapper<NullWritable, NullWritable> {

    private static final Logger logger = LoggerFactory.getLogger(MigrationMapper.class);

    protected final static String SERVER_CLASS = "com.despegar.p13n.hbasetool.TableWriter";

    public final static String TARGET_TABLE_NAME = "target_table_name";
    public final static String BATCH_SIZE = "put_batch_size";

    protected final static String HBASE_ZK_QUORUM = "HBASE_ZK_QUORUM";
    protected final static String HBASE_ZNODE_PARENT = "HBASE_ZNODE_PARENT";
    protected final static String FS_DEFAULT_NAME = "FS_DEFAULT_NAME";
    
	protected final static String RETRY_SLEEP_MILLISECONDS = "migrator.retry.sleep";
	protected final static String RETRY_ATTEMPTS = "migrator.retry.attempts";
	protected final static String MONITOR_PROGRESS = "migrator.progress";
	protected final static String DISABLE_WAL = "migrator.wal.disable";

    public final static String JAR = "h2_server_jar";

    private static final long LOG_EVERY = 10000;

    private Consumer consumer = null;

    private MyClassLoader serverLoader;

    private long counter = 0;
    
    private boolean monitorProgress = false;

    @Override
    public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        List<Map<String, Object>> values = Lists.newArrayList();

        for (KeyValue keyValue : value.raw()) {
            Map<String, Object> map = keyValue.toStringMap();
            map.put("row", keyValue.getRow());
            map.put("value", keyValue.getValue());
            // map.put("row", Base64.encodeBase64URLSafeString(keyValue.getRow()));
            // map.put("value", Base64.encodeBase64URLSafeString(keyValue.getValue()));
            values.add(map);
        }

        try {
            this.counter++;
            if (this.counter % LOG_EVERY == 0) {
                logger.info("exported {} rows", this.counter);
            }
            this.consumer.consume(values);
        } catch (Exception e) {
            logger.info("write failed with error " + e.getMessage(), e);
        }
        if (this.monitorProgress) {
        	context.progress();
        }

        // NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = value.getMap();
        // for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyEntry : map.entrySet()) {
        // byte[] familyByte = familyEntry.getKey();
        // NavigableMap<byte[], NavigableMap<Long, byte[]>> qualifierMap = familyEntry.getValue();
        // for (Map.Entry<byte[], NavigableMap<Long, byte[]>> qualifierEntry : qualifierMap.entrySet()) {
        // byte[] qualifierByte = qualifierEntry.getKey();
        // NavigableMap<Long, byte[]> versionMap = qualifierEntry.getValue();
        // for (Map.Entry<Long, byte[]> versionEntry : versionMap.entrySet()) {
        // Long version = versionEntry.getKey();
        // byte[] valueByte = versionEntry.getValue();
        // }
        // }
        // }

    }

    @Override
    protected void setup(Mapper<ImmutableBytesWritable, Result, NullWritable, NullWritable>.Context context)
        throws IOException {

        Configuration configuration = context.getConfiguration();
        String jarName = context.getConfiguration().get(JAR);
        String tableName = context.getConfiguration().get(TARGET_TABLE_NAME);
        String batchSizeString = context.getConfiguration().get(BATCH_SIZE);

        String zkQuorum = context.getConfiguration().get(HBASE_ZK_QUORUM);
        String zNodeParent = context.getConfiguration().get(HBASE_ZNODE_PARENT);
        String fsDefaultName = context.getConfiguration().get(FS_DEFAULT_NAME);
        
        String retrySleep = context.getConfiguration().get(RETRY_SLEEP_MILLISECONDS);
        String retryAttempts = context.getConfiguration().get(RETRY_ATTEMPTS);
        
        String progress = context.getConfiguration().get(MONITOR_PROGRESS);
        this.monitorProgress = Boolean.parseBoolean(progress);
        
        String disableWAL = context.getConfiguration().get(DISABLE_WAL);

        int batchSize = Integer.parseInt(batchSizeString);
        URL jar = null;
        try {
            logger.info("configuring job, searching file {} ", jarName);
            logger.info("tableName={}", tableName);
            logger.info("batchSize={}", batchSize);
            logger.info("zkQuorum={}", zkQuorum);
            logger.info("zNodeParent={}", zNodeParent);
            logger.info("fsDefaultName={}", fsDefaultName);
            logger.info("retrySleep={}", retrySleep);
            logger.info("retryAttempts={}", retryAttempts);
            logger.info("monitorProgress={}", this.monitorProgress);
            logger.info("disableWAL={}", disableWAL);
            
    	    System.setProperty(RETRY_SLEEP_MILLISECONDS, retrySleep);
    	    System.setProperty(RETRY_ATTEMPTS, retryAttempts);
    	    System.setProperty(DISABLE_WAL, disableWAL);

            Path[] localFiles = DistributedCache.getLocalCacheFiles(configuration);
            for (Path path : localFiles) {
                if (path.getName().endsWith(".jar")) {
                    String uri = "file:" + path.toString();
                    jar = new URL(uri);
                    logger.info("found server jar {}", jar);
                    break;
                }
            }

        } catch (Exception e) {
            logger.error("accessing server jar", e);
            throw new IOException(e);
        }
        if (jar == null) {
            String error = "Jar file not found, jar name: " + jarName;
            logger.error(error);
            throw new IOException(error);
        } else {
            this.initConsumer(jar, tableName, batchSize, zkQuorum, zNodeParent, fsDefaultName);
        }
    }

    @Override
    public void cleanup(Context context) throws IOException {
        logger.info("map completed, total exported rows={}", this.counter);
        try {
            this.consumer.flush();
            logger.info("flush success");
        } catch (Throwable e) {
            logger.error("flush error:" + e.getMessage(), e);
            if (e instanceof IOException) {
                throw e;
            } else {
                throw new IOException(e);
            }
        } finally {
            if (this.serverLoader != null) {
                this.serverLoader.close();
            }
        }
    }



    private Consumer initConsumer(URL jar, String tableName, Integer batchSize, String zkQuorum, String zNodeParent,
        String fsDefaultName) {
        if (this.consumer == null) {
            logger.info("initializing consumer");
            ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
            try {
                this.serverLoader = new MyClassLoader(new URL[] {jar}, currentClassLoader, Consumer.class);
                Thread.currentThread().setContextClassLoader(this.serverLoader);
                this.serverLoader.loadClass("org.apache.hadoop.util.ShutdownHookManager$2");
                Class<?> server = this.serverLoader.loadClass(SERVER_CLASS);
                this.consumer = (Consumer) server.getConstructor(String.class, Integer.class, String.class, String.class,
                    String.class).newInstance(tableName, batchSize, zkQuorum, zNodeParent, fsDefaultName);
                logger.info("consumer initialization ok");
            } catch (Throwable e) {
                logger.error("Error while launching server:" + e.getMessage(), e);
                throw new RuntimeException(e.getMessage());
            } finally {
                Thread.currentThread().setContextClassLoader(currentClassLoader);
            }
        }
        return this.consumer;
    }

}
