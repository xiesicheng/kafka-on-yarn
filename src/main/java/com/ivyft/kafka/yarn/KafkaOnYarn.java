package com.ivyft.kafka.yarn;

import com.ivyft.kafka.yarn.protocol.IdType;
import com.ivyft.kafka.yarn.protocol.KafkaYarnClient;
import com.ivyft.kafka.yarn.protocol.NodeContainer;
import com.ivyft.kafka.yarn.socket.FreeSocketPortFactory;
import com.ivyft.kafka.yarn.socket.SocketPortFactory;
import com.ivyft.kafka.yarn.util.NetworkUtils;
import kafka.metrics.KafkaMetricsConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.Utils;
import org.apache.avro.AvroRemoteException;
import org.apache.commons.cli.*;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Iterator;
import scala.collection.Seq;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/12/15
 * Time: 12:52
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class KafkaOnYarn {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaOnYarn.class);
    public final static String YARN_REPORT_WAIT_MILLIS = "yarn.report.wait.millis";
    public final static String MASTER_HEARTBEAT_INTERVAL_MILLIS = "master.heartbeat.interval.millis";
    public final static String KAFKA_MASTER_HOST = "yarn.kafka.master.host";
    public final static String MASTER_AVRO_PORT = "yarn.appmaster.avro.port";
    public final static String DEFAULT_KAFKA_NODE_NUM = "yarn.kafka.node.default.num";
    public final static String MASTER_CONTAINER_PRIORITY = "yarn.master.container.priority";


    protected final static HashMap<String, Command> commands = new HashMap<String, Command>();


    protected final static Command help = new Command("help", "print out this message") {


        @Override
        public Options getOpts() {
            Options options = new Options();
            options.addOption("h", "help", false, "show this message");
            return options;
        }

        @Override
        public void process(CommandLine cl) throws Exception {
            printHelpFor(cl.getArgList());
        }
    };


    protected final static Command KAFKA_ON_YARN = new Command("yarn", "kafka on yarn, start/shutdown app master") {


        /**
         * Kafka On Yarn, 默认的 App Name
         */
        private String appName = "KafkaOnYarn";


        /**
         * shutdown 需要的 appid
         */
        private String appId;


        /**
         * Yarn 中的线程队列名字
         */
        private String queue = "default";


        /**
         * App Master 默认的内存大小, -Xmx
         */
        private int amMB = 512;




        /**
         * App Kafka 默认的内存大小, -Xmx
         */
        private int kafkaMB = 512;


        /**
         * 默认的 Kafka 实例数
         */
        private int defaultInstance = 1;



        @Override
        public Options getOpts() {
            Options options = new Options();
            options.addOption("appid", "appid", true, "App Id, KafkaOnYarn ApplicationMaster ID");
            options.addOption("shutdown", "shutdown", false, "App Name, shutdown KafkaOnYarn");
            options.addOption("n", "appname", true, "App Name, KafkaOnYarn");
            options.addOption("im", "instance-memory", true, "Kafka Instance Memory, default 512M");
            options.addOption("ic", "instance-cores", true, "Kafka Instance Cores, default 1");
            options.addOption("q", "queue", true, "Hadoop Yarn Queue, default");
            options.addOption("m", "memory", true, "ApplicationMaster Memory, default 512M");
            options.addOption("c", "core", true, "ApplicationMaster Cores, default 1");
            options.addOption("s", false, "print exception");
            return options;
        }

        @Override
        public void process(CommandLine cl) throws Exception {
            if (cl.hasOption("shutdown")) {
                this.appId = cl.getOptionValue("appid");
                if (StringUtils.isBlank(appId)) {
                    throw new IllegalArgumentException("app id must not be null.");
                }

                ApplicationId applicationId = ConverterUtils.toApplicationId(appId);
                System.out.println(applicationId);
                shutdown(new KafkaConfiguration("kafka.yarn.properties"));
                return;
            }

            System.out.println("starting kafka on yarn, input Y/N");
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            String s = reader.readLine();
            if (!StringUtils.equals("Y", StringUtils.upperCase(s))) {
                return;
            }
            String appName = cl.getOptionValue("n");
            if (StringUtils.isNotBlank(appName)) {
                this.appName = appName;
            }

            String queue = cl.getOptionValue("q");
            if (StringUtils.isNotBlank(queue)) {
                this.queue = queue;
            }

            String m = cl.getOptionValue("m");
            if (StringUtils.isNotBlank(m)) {
                this.amMB = Integer.parseInt(m);
            }

            int appMasterCores = 1;
            int kafkaInstanceCores = 1;
            if(cl.hasOption("c")) {
                appMasterCores = Integer.parseInt(cl.getOptionValue("c"));

                if(appMasterCores < 1) {
                    appMasterCores = 1;
                }
            }
            if(cl.hasOption("ic")) {
                kafkaInstanceCores = Integer.parseInt(cl.getOptionValue("ic"));

                if(kafkaInstanceCores < 1) {
                    kafkaInstanceCores = 1;
                }
            }

            KafkaConfiguration conf = new KafkaConfiguration("kafka.yarn.properties");
            launchApplication(this.appName, this.queue, amMB, appMasterCores, conf,
                    kafkaMB, kafkaInstanceCores, defaultInstance);
        }


        public void shutdown(KafkaConfiguration conf) throws Exception {
            KafkaYarnClient client = attachToApp(appId,
                    conf).getClient();
            client.shutdown();
            client.close();
        }

    };





    protected final static Command KAFKA_ON_YARN_ADD = new Command("yarn-add", "kafka on yarn, add kafka instance") {


        private String appId;
        private int cores = 1;
        private int mb = 512;
        private String kafkaConf = "conf/server.properties";

        @Override
        public Options getOpts() {
            Options options = new Options();
            options.addOption("appid", "appid", true, "App Id, KafkaOnYarn ApplicationMaster ID");

            options.addOption("m", "memory", true, "Kafka Instance Memory, default 512M");
            options.addOption("c", "core", true, "Kafka Instance Cores, default 1");
            options.addOption("prop", true, "Kafka Instance Cores, default 1");

            options.addOption("s", false, "print exception");
            return options;
        }

        @Override
        public void process(CommandLine cl) throws Exception {
            this.appId = cl.getOptionValue("appid");
            if(StringUtils.isBlank(appId)) {
                throw new IllegalArgumentException("app id must not be null.");
            }

            ApplicationId applicationId = ConverterUtils.toApplicationId(appId);
            System.out.println(applicationId);

            String m = cl.getOptionValue("m");
            if(StringUtils.isNotBlank(m)) {
                this.mb = Integer.parseInt(m);
            }

            String c = cl.getOptionValue("c");
            if(StringUtils.isNotBlank(c)) {
                this.cores = Integer.parseInt(c);
            }

            String kafkaProp = cl.getOptionValue("prop");
            if(StringUtils.isNotBlank(kafkaProp)) {
                this.kafkaConf = kafkaProp;
            }

            KafkaConfiguration conf = new KafkaConfiguration("kafka.yarn.properties");
            KafkaYarnClient client = attachToApp(appId, conf).getClient();
            client.addInstance(this.mb,
                    this.cores,
                    this.kafkaConf
            );
            client.close();
        }
    };





    protected final static Command KAFKA_ON_YARN_LIST = new Command("yarn-list", "kafka on yarn, list kafka instance") {


        private String appId;


        @Override
        public Options getOpts() {
            Options options = new Options();
            options.addOption("appid", "appid", true, "App Id, KafkaOnYarn ApplicationMaster ID");
            options.addOption("s", false, "print exception");
            return options;
        }

        @Override
        public void process(CommandLine cl) throws Exception {
            String appid = cl.getOptionValue("appid");
            if (StringUtils.isNotBlank(appid)) {
                this.appId = appid;
            } else {
                throw new IllegalArgumentException("appid must not be null");
            }

            KafkaConfiguration conf = new KafkaConfiguration("kafka.yarn.properties");
            KafkaYarnClient client = attachToApp(appId, conf).getClient();
            List<NodeContainer> nodeContainers = client.listInstance();

            LOG.info("appid: " + appId + " kafka instance size=" + nodeContainers.size());
            for (NodeContainer container : nodeContainers) {
                System.out.println(container);
            }
            client.close();
        }
    };





    protected final static Command KAFKA_ON_YARN_STOP = new Command("yarn-stop", "kafka on yarn, stop kafka instance") {


        private String appId;


        private String containerId;


        private String nodeId;



        private boolean all = false;


        @Override
        public Options getOpts() {
            Options options = new Options();
            options.addOption("appid", "appid", true, "App Id, KafkaOnYarn ApplicationMaster ID");
            options.addOption("c", "containerid", true, "Kafka Container Id, KafkaOnYarn yarn-list");
            options.addOption("n", "nodeid", true, "Kafka Instance NodeId, KafkaOnYarn yarn-stop");
            options.addOption("a", "all", false, "Stop All Kafka Instance, --all");
            options.addOption("s", false, "print exception");
            return options;
        }

        @Override
        public void process(CommandLine cl) throws Exception {
            String appid = cl.getOptionValue("appid");
            if (StringUtils.isNotBlank(appid)) {
                this.appId = appid;
            } else {
                throw new IllegalArgumentException("appid must not be null");
            }

            if(cl.hasOption("a")) {
                all = true;
            }

            if(!all) {
                if(cl.hasOption("c")) {
                    this.containerId = cl.getOptionValue("c");
                }

                if(cl.hasOption("n")) {
                    this.nodeId = cl.getOptionValue("n");
                }

                if (StringUtils.isBlank(containerId) && StringUtils.isBlank(nodeId)) {
                    throw new IllegalArgumentException("containerId or nodeId has value, not any null");
                }

                KafkaConfiguration conf = new KafkaConfiguration("kafka.yarn.properties");
                KafkaYarnClient client = attachToApp(appId, conf).getClient();
                if(StringUtils.isNotBlank(containerId)) {
                    client.stopInstance(containerId, IdType.CONTAINER_ID);
                } else if(StringUtils.isNotBlank(nodeId)) {
                    client.stopInstance(nodeId, IdType.NODE_ID);
                }
                client.close();
            } else {
                // stop all
                KafkaConfiguration conf = new KafkaConfiguration("kafka.yarn.properties");
                KafkaYarnClient client = attachToApp(appId, conf).getClient();
                client.stopAll();
                client.close();
            }
        }
    };






    protected final static Command KAFKA = new Command("start-kafka", "start kafka") {


        @Override
        public Options getOpts() {
            Options options = new Options();
            options.addOption("app_attempt_id", true, "App Attempt ID. Not to be used " +
                    "unless for testing purposes");
            options.addOption("id", true, "broker.id");
            options.addOption("logdir", true, "kafka log dir");
            options.addOption("pfile", true, "kafka properties file");
            options.addOption("p", "port", true, "kafka broker port");

            options.addOption("s", false, "print exception");
            return options;
        }

        @Override
        public void process(CommandLine cl) throws Exception {
            LOG.info("Starting the Kafka!!!!");
            ApplicationAttemptId appAttemptID;
            Map<String, String> envs = System.getenv();
            if (cl.hasOption("app_attempt_id")) {
                String appIdStr = cl.getOptionValue("app_attempt_id");
                appAttemptID = ConverterUtils.toApplicationAttemptId(appIdStr);
            } else if (envs.containsKey(ApplicationConstants.Environment.CONTAINER_ID.name())) {
                ContainerId containerId = ConverterUtils.toContainerId(envs
                        .get(ApplicationConstants.Environment.CONTAINER_ID.name()));
                appAttemptID = containerId.getApplicationAttemptId();
                LOG.info("appAttemptID from env:" + appAttemptID.toString());
            } else {
                LOG.error("appAttemptID is not specified for kafka instance.");
                throw new Exception("appAttemptID is not specified for kafka instance.");
            }

            String brokerId = cl.getOptionValue("id");
            String logDir = cl.getOptionValue("logdir");
            String kafkaPort = cl.getOptionValue("p");
            String pfile = cl.getOptionValue("pfile");

            if(StringUtils.isBlank(brokerId)) {
                throw new IllegalArgumentException("id must not be blank.");
            }


            LOG.info("container id: " + appAttemptID);
            KafkaConfiguration conf = new KafkaConfiguration();

            String kafkaHost = System.getProperty("kafka.host", NetworkUtils.getLocalhostName());
            if(StringUtils.isNotBlank(kafkaPort)) {
                int port = Integer.parseInt(kafkaPort);

                SocketPortFactory factory = new FreeSocketPortFactory();
                port = factory.getSocketPort(port, conf.getInt("kafka.port.step", 1));

                System.setProperty("kafka.port", String.valueOf(port));
                conf.setProperty("kafka.port", String.valueOf(port));
                kafkaPort = String.valueOf(port);
            } else {
                int port = conf.getInt(kafkaPort, 9092);

                SocketPortFactory factory = new FreeSocketPortFactory();
                port = factory.getSocketPort(port, conf.getInt("kafka.port.step", 1));

                System.setProperty("kafka.port", String.valueOf(port));
                conf.setProperty("kafka.port", String.valueOf(port));
                kafkaPort = String.valueOf(port);
            }

            LOG.info("-Dkafka.port=" + System.getProperty("kafka.port"));

            String pwd = System.getProperty("kafka.home");
            LOG.info("kafka.home=" + pwd);

            File kafkaHome = new File(pwd);
            File kafkaLogDir = new File(kafkaHome, "logs");

            if(!kafkaLogDir.exists()) {
                LOG.info("mkdir " + kafkaLogDir.getAbsolutePath());
                kafkaLogDir.mkdirs();
            }

            try {
                Properties props = Utils.loadProps(pfile);
                props.setProperty("broker.id", brokerId);
                props.setProperty("port", kafkaPort);
                props.setProperty("log.dirs", kafkaLogDir.getAbsolutePath());



                KafkaConfig serverConfig = new KafkaConfig(props);

                startReporters(serverConfig);
                final KafkaServerStartable kafkaServerStartable = new KafkaServerStartable(serverConfig);

                // attach shutdown handler to catch control-c
                Runtime.getRuntime().addShutdownHook(new Thread() {
                    @Override
                    public void run() {
                        kafkaServerStartable.shutdown();
                    }
                });
                LOG.info("start kafka server success.");
                kafkaServerStartable.startup();
                kafkaServerStartable.awaitShutdown();
            } catch (Exception e) {
                LOG.error(ExceptionUtils.getFullStackTrace(e));
                System.exit(143);
            }
        }
    };



    public static void startReporters(final KafkaConfig serverConfig) {
        //KafkaMetricsReporter kafkaMetricsReporter = new KafkaMetricsReporter();

        KafkaMetricsConfig metricsConfig = new KafkaMetricsConfig(serverConfig.props());
        if(metricsConfig.reporters().size() > 0) {
            Seq<String> reporters = metricsConfig.reporters();
            Iterator<String> iterator = reporters.iterator();
            while (iterator.hasNext()) {
                String reporterType = iterator.next();
                //Seq<Object> objectSeq = new Seq();
                //objectSeq.addString(new scala.collection.mutable.StringBuilder().append(reporterType));

               // KafkaMetricsReporter reporter = Utils.createObject(KafkaMetricsReporter.class.getName(), objectSeq);
                //reporter.init(serverConfig.props());

                //if (reporter instanceof KafkaMetricsReporterMBean) {
                //    Utils.registerMBean(reporter, ((KafkaMetricsReporterMBean) reporter).getMBeanName());
                //}
            }
        }
    }


    public static void printHelpFor(Collection<String> args) {
        if (args == null || args.size() < 1) {
            args = commands.keySet();
        }
        HelpFormatter f = new HelpFormatter();
        for (String command : args) {
            Command c = commands.get(command);
            if (c != null) {
                f.printHelp(command, c.getHeaderDescription(), c.getOpts(), null);
                System.out.println();
            } else {
                System.err.println("ERROR: " + c + " is not a supported command.");
            }
        }
    }

    private static String printUsageFooter() {
        return "\nGlobal Options:" +
                "  -s\t\tshow stacktrace\n";
    }

    private static void printUsageHeader() {
        System.err.println("Usage: ");
    }


    private static String[] removeArg(String[] args, String argToRemove) {
        List<String> newArgs = new ArrayList<String>();
        for (String arg : args) {
            if (!arg.equals(argToRemove)) {
                newArgs.add(arg);
            }
        }
        return newArgs.toArray(new String[newArgs.size()]);
    }


    protected static Map<String, String> parseOptionMap(final String[] args) {
        Map<String, String> optionMap = new HashMap<String, String>();
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("-")) {
                String value = null;
                if (i < args.length - 1 && !args[i + 1].startsWith("-")) {
                    value = args[i + 1];
                }
                optionMap.put(args[i], value);
            }
        }
        return optionMap;
    }


    private static void printError(String errorMsg) {
        System.err.println("ERROR: " + errorMsg);
    }


    //private ApplicationClientProtocol appClientProtocol;
    private YarnClient _yarn;
    private YarnConfiguration _hadoopConf;
    private ApplicationId _appId;
    private KafkaConfiguration conf;
    private KafkaYarnClient _client = null;

    private KafkaOnYarn(KafkaConfiguration kafkaConf) {
        this(null, kafkaConf);
    }

    private KafkaOnYarn(ApplicationId appId, KafkaConfiguration kafkaConf) {
        _hadoopConf = new YarnConfiguration();
        _yarn = YarnClient.createYarnClient();
        this.conf = kafkaConf;
        _appId = appId;
        _yarn.init(_hadoopConf);
        _yarn.start();
    }

    public void stop() {
        if (_client != null) {
            try {
                _client.shutdown();
            } catch (AvroRemoteException e) {
                LOG.error(e.getMessage());
            }
        }
        _yarn.stop();
    }

    public ApplicationId getAppId() {
        return _appId;
    }

    public synchronized KafkaYarnClient getClient() throws YarnException, IOException {
        if (_client == null) {
            String host = null;
            int port = 0;
            //wait for application to be ready
            int max_wait_for_report = conf.getInt(YARN_REPORT_WAIT_MILLIS, 60000);
            int waited = 0;
            while (waited < max_wait_for_report) {
                ApplicationReport report = _yarn.getApplicationReport(_appId);
                host = report.getHost();
                port = report.getRpcPort();
                LOG.info("application master start at " + host + ":" + port);
                if (host == null || port == 0) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                    waited += 1000;
                } else {
                    break;
                }
            }
            if (host == null || port == 0) {
                LOG.info("No host/port returned for Application Master " + _appId);
                return null;
            }

            LOG.info("application report for " + _appId + " :" + host + ":" + port);
            LOG.info("Attaching to " + host + ":" + port + " to talk to app master " + _appId);
            _client = new KafkaYarnClient(host, port);
        }
        return _client;
    }


    /**
     * 启动 AppMaster
     * @param appName
     * @param queue
     * @param amMB
     * @param kafkaZip
     * @throws Exception
     */
    private void launchApp(String appName,
                           String queue,
                           int amMB,
                           int masterCores,
                           int kafkaMB,
                           int kafkaCores,
                           int defaultInstance) throws Exception {
        LOG.info("KafkaOnYarn:launchApp() ...");
        YarnClientApplication client_app = _yarn.createApplication();
        GetNewApplicationResponse app = client_app.getNewApplicationResponse();
        _appId = app.getApplicationId();
        LOG.info("_appId:" + _appId);

        if (amMB > app.getMaximumResourceCapability().getMemory()) {
            //TODO need some sanity checks
            amMB = app.getMaximumResourceCapability().getMemory();
        }


        ApplicationSubmissionContext appContext =
                Records.newRecord(ApplicationSubmissionContext.class);
        appContext.setApplicationId(app.getApplicationId());
        appContext.setApplicationName(appName);
        appContext.setQueue(queue);
        //appContext.setUnmanagedAM(true);
        
       
        // Set the priority for the application master
        Priority pri = Records.newRecord(Priority.class);
        pri.setPriority(0);
        appContext.setPriority(pri);
  

        // Set up the container launch context for the application master
        ContainerLaunchContext amContainer = Records
                .newRecord(ContainerLaunchContext.class);
        Credentials credentials = new Credentials();
        DataOutputBuffer dob = new DataOutputBuffer();
        credentials.writeTokenStorageToStream(dob);
        ByteBuffer securityTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
        //security tokens for HDFS distributed cache
        amContainer.setTokens(securityTokens);


        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(amMB);
        capability.setVirtualCores(masterCores);
        appContext.setResource(capability);

        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

        // set local resources for the application master
        // local files or archives as needed
        // In this scenario, the jar file for the application master is part of the
        // local resources
        LOG.info("Copy App Master jar from local filesystem and add to local environment");
        // Copy the application master jar to the filesystem
        // Create a local resource to point to the destination jar path
        String appMasterJar = findContainingJar(KafkaAppMaster.class);
        LOG.info("appMasterJar: " + appMasterJar);

        FileSystem fs = FileSystem.get(_hadoopConf);

        Path src = new Path(appMasterJar);
        String appHome = Util.getApplicationHomeForId(_appId.toString());
        Path dst = new Path(fs.getHomeDirectory(),
                appHome + Path.SEPARATOR + "AppMaster.jar");
        fs.copyFromLocalFile(false, true, src, dst);
        LOG.info("copy jar from: " + src + " to: " + dst);
        localResources.put("AppMaster.jar", Util.newYarnAppResource(fs, dst));

        String kafkaLibPath = conf.getString("yarn.kafka.appmaster.lib.path");
        LOG.info("kafka libs path: " + kafkaLibPath);

        Path zip;
        if (StringUtils.isNotBlank(kafkaLibPath)) {
            //自己指定的
            zip = new Path(kafkaLibPath);
            if (!fs.exists(zip) || !fs.isFile(zip)) {
                throw new IllegalArgumentException("kafka location not exists. " + kafkaLibPath);
            }
        } else {
            throw new IllegalStateException("conf yarn.kafka.node.lib.path must not be blank.");
        }

        LocalResourceVisibility visibility = LocalResourceVisibility.PUBLIC;
        conf.setProperty("kafka.zip.path", zip.makeQualified(fs).toUri().getPath());
        conf.setProperty("kafka.zip.visibility", "PUBLIC");
        if (!Util.isPublic(fs, zip)) {
            visibility = LocalResourceVisibility.APPLICATION;
            conf.setProperty("kafka.zip.visibility", "APPLICATION");
        }
        localResources.put("lib", Util.newYarnAppResource(fs, zip, LocalResourceType.ARCHIVE, visibility));

        Path confDst = Util.copyClasspathConf(fs, appHome);
        // establish a symbolic link to conf directory
        localResources.put("conf", Util.newYarnAppResource(fs, confDst));

        int rs = 3;
        // Setup security tokens
        Path[] paths = new Path[rs];
        paths[0] = dst;
        paths[1] = zip;
        paths[2] = confDst;


        TokenCache.obtainTokensForNamenodes(credentials, paths, _hadoopConf);

        // Set local resource info into app master container launch context
        amContainer.setLocalResources(localResources);

        // Set the env variables to be setup in the env where the application master
        // will be run
        LOG.info("Set the environment for the application master");
        Map<String, String> env = new HashMap<String, String>();
        // add the runtime classpath needed for tests to work
        Apps.addToEnvironment(env, Environment.CLASSPATH.name(), "./AppMaster.jar");
        Apps.addToEnvironment(env, Environment.CLASSPATH.name(), "./conf");
        Apps.addToEnvironment(env, Environment.CLASSPATH.name(), "./lib/*");

        String yarnClasspath = StringUtils.trimToNull(_hadoopConf.get("yarn.application.classpath", ""));
        if(StringUtils.isNotBlank(yarnClasspath)) {
            String[] strs = yarnClasspath.split(",", 0);
            for (String str : strs) {
                Apps.addToEnvironment(env, Environment.CLASSPATH.name(), StringUtils.trim(str));
            }

        }

        String java_home = conf.getProperty("kafka.yarn.java_home", "");
        if (StringUtils.isNotBlank(java_home)) {
            env.put("JAVA_HOME", java_home);
        }

        LOG.info("Using JAVA_HOME = [" + env.get("JAVA_HOME") + "]");

        env.put("appId", _appId.toString());
        env.put("KAFKA_LOG_DIR", ApplicationConstants.LOG_DIR_EXPANSION_VAR);

        amContainer.setEnvironment(env);

        // Set the necessary command to execute the application master
        Vector<String> vargs = new Vector<String>();
        vargs.add(Environment.JAVA_HOME.$() + "/bin/java");
        vargs.add("-Xmx" + amMB + "m");
        vargs.add("-Xms" + amMB + "m");
        vargs.add(conf.getString("kafka.yarn.app.master.opts", ""));
        //vargs.add("-Dkafka.node.hostname.overwritten=$hostname");
        vargs.add("-Dkafka.root.logger=INFO,DRFA");
        vargs.add("-Dkafka.log.dir=" + ApplicationConstants.LOG_DIR_EXPANSION_VAR);
        vargs.add("-Dkafka.log.file=kafka-on-yarn.log");
        vargs.add("-DappId=" + getAppId().toString());
        vargs.add("-DappJar=" + dst.toUri().getPath());
        vargs.add("-DappName=" + appName);
        vargs.add("-Dqueue=" + queue);
        vargs.add("-Dkafka.instance.memory=" + kafkaMB);
        vargs.add("-Dkafka.instance.cores=" + kafkaCores);
        //vargs.add("-verbose:class");
        vargs.add(com.ivyft.kafka.yarn.KafkaAppMaster.class.getName());
        vargs.add("-num " + defaultInstance);

        vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
        vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");
        // Set java executable command
        LOG.info("Setting up app master command:" + vargs);

        amContainer.setCommands(vargs);


        appContext.setAMContainerSpec(amContainer);
        _yarn.submitApplication(appContext);
    }


    /**
     * Wait until the application is successfully launched
     *
     * @throws YarnException
     */
    public boolean waitUntilLaunched() throws YarnException, IOException {
        while (true) {
            // Get application report for the appId we are interested in
            ApplicationReport report = _yarn.getApplicationReport(_appId);

            LOG.info(report.toString());


            YarnApplicationState state = report.getYarnApplicationState();
            FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
            if (YarnApplicationState.FINISHED == state) {
                if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
                    LOG.info("Application has completed successfully. Breaking monitoring loop");
                    return true;
                } else {
                    LOG.info("Application did finished unsuccessfully."
                            + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
                            + ". Breaking monitoring loop");
                    return false;
                }
            } else if (YarnApplicationState.KILLED == state
                    || YarnApplicationState.FAILED == state) {
                LOG.info("Application did not finish."
                        + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
                        + ". Breaking monitoring loop");
                return false;
            }

            // Check app status every 1 second.
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                LOG.debug("Thread sleep in monitoring loop interrupted");
            }

            //announce application master's host and port
            if (state == YarnApplicationState.RUNNING) {
                LOG.info(report.getApplicationId() + " luanched, status: " + state);
                return true;
            }
        }
    }


    /**
     * Find a jar that contains a class of the same name, if any.
     * It will return a jar file, even if that is not the first thing
     * on the class path that has a class with the same name.
     *
     * @param my_class the class to find.
     * @return a jar file that contains the class, or null.
     * @throws IOException on any error
     */
    public static String findContainingJar(Class<?> my_class) throws IOException {
        String appJar = System.getProperty("app.jar");
        if (StringUtils.isNotBlank(appJar)) {
            //kafka-yarn/target/kafka-yarn.jar
            return new File(appJar).toURI().toString();
        }
        ClassLoader loader = my_class.getClassLoader();
        String class_file = my_class.getName().replaceAll("\\.", "/") + ".class";
        for (Enumeration<URL> itr = loader.getResources(class_file);
             itr.hasMoreElements(); ) {
            URL url = itr.nextElement();
            if ("jar".equals(url.getProtocol())) {
                String toReturn = url.getPath();
                if (toReturn.startsWith("file:")) {
                    toReturn = toReturn.substring("file:".length());
                }
                // URLDecoder is a misnamed class, since it actually decodes
                // x-www-form-urlencoded MIME type rather than actual
                // URL encoding (which the file path has). Therefore it would
                // decode +s to ' 's which is incorrect (spaces are actually
                // either unencoded or encoded as "%20"). Replace +s first, so
                // that they are kept sacred during the decoding process.
                toReturn = toReturn.replaceAll("\\+", "%2B");
                toReturn = URLDecoder.decode(toReturn, "UTF-8");
                return toReturn.replaceAll("!.*$", "");
            }
        }
        throw new IOException("Fail to locat a JAR for class: " + my_class.getName());
    }




    public static KafkaOnYarn launchApplication(String appName,
                                                String queue,
                                                int amMB,
                                                int appMasterCores,
                                                KafkaConfiguration kafkaConf,
                                                int kafkaMB,
                                                int kafkaCores,
                                                int defaultInstance) throws Exception {
        KafkaOnYarn kafkaOnYarn = new KafkaOnYarn(kafkaConf);
        //TODO
        kafkaOnYarn.launchApp(appName, queue, amMB, appMasterCores,
                kafkaMB, kafkaCores, defaultInstance);
        kafkaOnYarn.waitUntilLaunched();
        return kafkaOnYarn;
    }

    public static KafkaOnYarn attachToApp(String appId, KafkaConfiguration kattaConf) {
        return new KafkaOnYarn(ConverterUtils.toApplicationId(appId), kattaConf);
    }


    public static void main(String[] args) throws ParseException {
        commands.put(help.getCommand(), help);
        commands.put(KAFKA_ON_YARN.getCommand(), KAFKA_ON_YARN);
        commands.put(KAFKA_ON_YARN_ADD.getCommand(), KAFKA_ON_YARN_ADD);
        commands.put(KAFKA_ON_YARN_LIST.getCommand(), KAFKA_ON_YARN_LIST);
        commands.put(KAFKA_ON_YARN_STOP.getCommand(), KAFKA_ON_YARN_STOP);
        commands.put(KAFKA.getCommand(), KAFKA);


        String commandName = null;
        String[] commandArgs = null;
        if (args.length == 0) {
            commandName = "help";
            commandArgs = new String[0];
        } else {
            commandName = args[0];
            if(args.length > 1) {
                commandArgs = Arrays.copyOfRange(args, 1, args.length);
            } else {
                commandArgs = new String[0];
            }
        }

        Command command = commands.get(commandName);
        if (command == null) {
            LOG.error("ERROR: " + commandName + " is not a supported command.");
            printHelpFor(null);
            System.exit(1);
        }
        CommandLine cl = new GnuParser().parse(command.getOpts(), commandArgs);
        if (cl.hasOption("h") || cl.hasOption("help")) {
            printHelpFor(Arrays.asList(commandName));
            return;
        }

        boolean showStackTrace = cl.hasOption("s");
        try {
            command.process(cl);
        } catch (Exception e) {
            if (showStackTrace) {
                e.printStackTrace();
            } else {
                printError(e.getMessage());
            }
            HelpFormatter f = new HelpFormatter();
            f.printHelp(command.getCommand(),
                    command.getHeaderDescription(),
                    command.getOpts(),
                    showStackTrace ? null : printUsageFooter());
            System.exit(1);
        }
    }

}
