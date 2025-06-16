package benchmarktool;

import static java.lang.System.exit;
import static java.lang.Thread.sleep;
import static org.fusesource.leveldbjni.JniDBFactory.factory;


import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import io.prometheus.client.exporter.HTTPServer;
import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Logger;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.Snapshot;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.LoggerFactory;

public class App {
    private static String DB_TYPE = "db_type";
    private static String DB_PATH = "db_path";
    private static String FILE_KEY = "100wKey.bin";
    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private static final int RANDOM_LENGTH = 10;
    private static  int THREAD_BEGIN = 0;
    private static  int THREAD_INC_TIME = 10;
    private static  boolean RandomI = true;
    private static List<byte[]> keyList = new ArrayList<>();
    static final Histogram requestDuration = Histogram.build()
        .name("request")
        .help("Request duration in seconds.")
        .buckets(0.000001,0.000002,0.000003,0.000004,0.000005,0.000006,0.000007,0.000008,0.000009,0.00001,0.00002,0.00003,0.00004,0.00005,0.00006,0.00007,0.00008,0.00009,0.0001,0.0005,0.001,0.01)
        .register();
    static final Counter levelDBCounter = Counter.build().name("db_event")
            .help("leveldb event counter")
        .labelNames("event")
        .register();
    // .buckets(0.1, 0.5, 1.0, 2.0,100,200,500,1000,10000) // 微妙us
    // 使用 SLF4J 创建日志器
    private static final org.slf4j.Logger innerLogger = LoggerFactory.getLogger(App.class);
//
    private static Logger leveldbLogger = message -> {
        innerLogger.info("{} {}","leveldb", message);
        if (message.startsWith("Recovering")) {
            levelDBCounter.labels("recover").inc();
        }
        if (message.startsWith("Compacting")) {
            levelDBCounter.labels("compact").inc();
        }
        if (message.startsWith("Delete")) {
            levelDBCounter.labels("delete").inc();
        }
        if (message.startsWith("Generated")) {
            levelDBCounter.labels("create").inc();
        }
    };

    public static void main(String[] args) {
        stress(args);
        while(true);
    }

    public static void stress(String[] args) {
        Thread prometheusThead=startPrometheusMetricsServer();
        try {
            sleep(3*1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Options options = new Options();
        options.addOption(DB_TYPE, true, "Database type (leveldb/rocksdb)");
        options.addOption(DB_PATH, true, "Database type (leveldb/rocksdb)");
        options.addOption("threadBegin", true, "Number of threads initially");
        options.addOption("thread", true, "Number of concurrent threads");
        options.addOption("threadIncTime", true, "Number of  threads increment duration");
        options.addOption("duration", true, "Duration in seconds");
        options.addOption("geneKey", false, "gene100wKey");
        options.addOption("geneKeyNum", true, "生成key大小");
        options.addOption("random", true, "是否随机起始位置");
        options.addOption("iter", true, "统计前缀数量");

        options.addOption("help", false, "Print help");
        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cmd = parser.parse(options, args);
            if (cmd.hasOption("help")) {
                new HelpFormatter().printHelp("BenchmarkTool", options);
                System.out.println("help");
                return;
            }
            if (cmd.hasOption("geneKey")) {
               int geneKeyNum = Integer.parseInt(cmd.getOptionValue("geneKeyNum", "10000"));
                Gene100wKey(cmd.getOptionValue(DB_PATH, "/tmp/test_db"),geneKeyNum);
                System.out.println("geneKey success");
                return;
            }
            else if(cmd.hasOption("iter")){
                String iter = cmd.getOptionValue("iter", "");
                iterCount(cmd.getOptionValue(DB_PATH, "/tmp/test_db"),iter);
                return ;
            }
            keyList = BinaryFileLoader.loadFromFile(FILE_KEY);
            String dbType = cmd.getOptionValue(DB_TYPE, "leveldb");
            int threads = Integer.parseInt(cmd.getOptionValue("thread", "2"));
            THREAD_BEGIN = Integer.parseInt(cmd.getOptionValue("threadBegin", "1"));
            THREAD_INC_TIME = Integer.parseInt(cmd.getOptionValue("threadIncTime", "10"));
            int duration = Integer.parseInt(cmd.getOptionValue("duration", "10"));
            String path =cmd.getOptionValue(DB_PATH, "/tmp/test_db");
            RandomI =Boolean.parseBoolean(cmd.getOptionValue("random", "true"));

            System.out.printf("dbType: %s,random:%s\n" , dbType,RandomI);
            System.out.printf("threads:%d,threadBegin:%d,threadIncTime:%d s\n" , threads,THREAD_BEGIN,THREAD_INC_TIME);
            System.out.println("duration: " + duration);
            System.out.println("path: " + path);

            // 执行压测
            runBenchmark(dbType,path, threads, duration);

        } catch (ParseException e) {
            System.err.println("参数解析失败: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            exit(1);
        }
        try {
            //TODO 无效
            prometheusThead.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static Thread startPrometheusMetricsServer() {
        // 创建一个新的线程来运行 Prometheus 指标服务器
        Thread metricsThread = new Thread(() -> {
            try {
                // 启动HTTP服务器，默认端口9095
                HTTPServer server = new HTTPServer(9095);
            } catch (IOException  e) {
                e.printStackTrace();
            }
        });
        metricsThread.start();
        return metricsThread;
    }

    private static void printTime(){
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        String formattedTime = now.format(formatter);
        System.out.println("格式化时间：" + formattedTime);
    }
    private static void Gene100wKey(String path, int reservoirSize){
        DB db = null;
        try {
            db = factory.open(new File(path), new org.iq80.leveldb.Options());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        db.getProperty("leveldb.stats");
        // 创建快照以确保一致性
        Snapshot snapshot = db.getSnapshot();
        ReadOptions readOptions = new ReadOptions().snapshot(snapshot);

        List<byte[]> reservoir = new ArrayList<>(reservoirSize);
        printTime();
        try (DBIterator iterator = db.iterator(readOptions)) {
            iterator.seekToFirst();
            long count = 0;

            while (iterator.hasNext()) {
                byte[] key = iterator.peekNext().getKey();
                count++;
                // 填充蓄水池前reservoirSize个元素
                if (count <= reservoirSize) {
                    reservoir.add(Arrays.copyOf(key, key.length));
                } else {
                    // 生成随机数决定是否替换蓄水池中的元素
                    int r = ThreadLocalRandom.current().nextInt((int) count);
                    if (r < reservoirSize) {
                        reservoir.set(r, Arrays.copyOf(key, key.length));
                    }
                }
                iterator.next();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            // 释放快照和数据库资源
            try {
                snapshot.close();
                db.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }
        printTime();
        System.out.println("reservoir size: "+reservoir.size());
        // 使用固定随机种子（确保结果可复现）
        long seed = 123L;
        ThreadLocalRandom.current().setSeed(seed);
        Collections.shuffle(reservoir, ThreadLocalRandom.current());

        BinaryFileWriter binaryFileWriter=new BinaryFileWriter();
        try {
            binaryFileWriter.writeToFile(FILE_KEY,reservoir);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private static void Gene100wKey(String path){
        // 初始化蓄水池容量为100万
        int reservoirSize = 1_000_000;
        Gene100wKey(path,reservoirSize);

    }
    // options
    //      createIfMissing = true,
    //      paranoidChecks = true,
    //      verifyChecksums = true,
    //      compressionType = 1,        // compressed with snappy
    //      blockSize = 4096,           // 4  KB =         4 * 1024 B
    //      writeBufferSize = 10485760, // 10 MB = 10 * 1024 * 1024 B
    //      cacheSize = 10485760,       // 10 MB = 10 * 1024 * 1024 B
    //       maxOpenFiles = 1000
    private static DB openLevelDB(String path){
        org.iq80.leveldb.Options options = new org.iq80.leveldb.Options();
        options.createIfMissing(true);
        options.paranoidChecks(true);
        options.verifyChecksums(true);
        options.compressionType(CompressionType.SNAPPY);
        options.blockSize(4096);
        options.writeBufferSize(10485760);
        options.cacheSize(10485760);
        options.maxOpenFiles(1000);
//        //
         options.logger(leveldbLogger);
//        options.logger()
        DB levelDb=null;
        try {
          levelDb=  factory.open(new File(path), options);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return levelDb;
    }
    private static void runBenchmark(String dbType,String path, int threads, int duration ) {
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        AtomicLong totalQueries = new AtomicLong(0);
        AtomicLong totalTime = new AtomicLong(0);

        long startTime = System.currentTimeMillis();
        long endTime = startTime + duration * 1000;
        DB leveldb=null;
        RocksDB rocksdb=null;
        try {
            if (dbType.equalsIgnoreCase("leveldb")) {
                leveldb = openLevelDB(path);
            } else {
                rocksdb = RocksDB.open(path);
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        for (int i = 1; i <= threads; i++) {
            DB finalLeveldb = leveldb;
            RocksDB finalRocksdb = rocksdb;
            int finalI = i-1;
            int keyLength =  keyList.size()/threads;
            executor.submit(() -> {
                try {
                    int j = 0;
                    if (RandomI) {
                         j = ThreadLocalRandom.current().nextInt(keyLength);
                         innerLogger.debug("random j={}",j);
                    }
                    if (dbType.equalsIgnoreCase("leveldb")) {

                        while (System.currentTimeMillis() < endTime) {
                            int index= keyList.size()/threads*finalI+j;
                            if (index>=keyList.size()){
                                j=0;
                                index= keyList.size()/threads*finalI+j;
                            }
                            byte[] key=keyList.get(index);
                            Histogram.Timer timer = requestDuration.startTimer();
                            long queryStart = System.nanoTime();
                            finalLeveldb.get(key); // 查询不存在键
                            long queryEnd = System.nanoTime();
                            timer.observeDuration();
                            totalQueries.incrementAndGet();
                            totalTime.addAndGet(queryEnd - queryStart);
                            j++;
                        }
                    } else {
                        while (System.currentTimeMillis() < endTime) {
                            int index= keyList.size()/threads*finalI+j;
                            if (index>=keyList.size()){
                                j=0;
                                index= keyList.size()/threads*finalI+j;
                            }
                            byte[] key=keyList.get(index);
                            Histogram.Timer timer = requestDuration.startTimer();
                            long queryStart = System.nanoTime();
                            finalRocksdb.get(key);
                            long queryEnd = System.nanoTime();
                            timer.observeDuration();
                            totalQueries.incrementAndGet();
                            totalTime.addAndGet(queryEnd - queryStart);
                            j++;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            if (THREAD_BEGIN!=0 && i>=THREAD_BEGIN){
                try {
                    System.out.printf("sleep after thread %d\n",i);
                    sleep(THREAD_INC_TIME*1000);
                } catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
            }
        }

        executor.shutdown();
        try {
            executor.awaitTermination(duration + 10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 输出结果
        long totalTimeMs = totalTime.get() / 1_000_000; // 转换为毫秒
        long totalTimeUs = totalTime.get() / 1_000; // 转换为毫秒
        double avgLatency = totalQueries.get() == 0 ? 0 : (double) totalTimeMs / totalQueries.get();
        double avgLatencyUs = totalQueries.get() == 0 ? 0 : (double) totalTimeUs / totalQueries.get();
        System.out.printf("总查询次数: %d\n平均延迟: %.3f ms 即 %.3f us\n吞吐量: %.2f qps\n",
            totalQueries.get(), avgLatency,avgLatencyUs, totalQueries.get() / (double) duration);
    }

    public static byte[] getKey(int threadCount,int threadNum,int threadTmp) {
       int index= keyList.size()/threadCount*threadNum+threadTmp;
       return keyList.get(index);
    }
    public static byte[] generate(String prefix) {
        StringBuilder sb = new StringBuilder(prefix);
        for (int i = 0; i < RANDOM_LENGTH; i++) {
            // 从字符集中随机选取一个字符
            int index = ThreadLocalRandom.current().nextInt(CHARACTERS.length());
            sb.append(CHARACTERS.charAt(index));
        }
        return sb.toString().getBytes();
    }
    private static void iterCount(String path,String hexPrefix){
        DB db = null;
        try {
            db = factory.open(new File(path), new org.iq80.leveldb.Options());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        iterCount(hexPrefix,db);
    }
    private static void iterCount(String hexAddress, DB db) {
        byte[] prefixKey = CommonUtil.fromHexString(hexAddress);
        DBIterator iterator = db.iterator();
        int i=0;
        for (iterator.seek(prefixKey); iterator.hasNext(); iterator.next()) {
            if(i<3){
                Map.Entry<byte[], byte[]> entry = iterator.peekNext();
                innerLogger.info("prefix:{},example {}",hexAddress,CommonUtil.bytesToHex(entry.getKey()));
            }
            i++;
        }
        innerLogger.info("prefix:{},count {}",hexAddress,i);
        System.out.printf("prefix:%s,count %d",hexAddress,i);
    }

}