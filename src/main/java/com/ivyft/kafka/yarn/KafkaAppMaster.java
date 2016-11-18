/*
 * Copyright (c) 2013 Yahoo! Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package com.ivyft.kafka.yarn;

import com.ivyft.kafka.yarn.protocol.KafkaYarnAvroServer;
import com.ivyft.kafka.yarn.protocol.KafkaYarnMasterProtocol;
import com.ivyft.kafka.yarn.protocol.KafkaYarnProtocol;
import com.ivyft.kafka.yarn.socket.FreeSocketPortFactory;
import com.ivyft.kafka.yarn.socket.SocketPortFactory;
import com.ivyft.kafka.yarn.util.NetworkUtils;
import org.apache.avro.AvroRemoteException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/11/28
 * Time: 19:51
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class KafkaAppMaster implements Runnable, AMRMClientAsync.CallbackHandler {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaAppMaster.class);


    /**
     * 默认 Kafka Instance 在 Yarn 任务优先级
     */
    private final Priority DEFAULT_PRIORITY = Records.newRecord(Priority.class);


    /**
     * AppMaster 当前是否调用 Stop() 方法, 防止重复调用
     */
    private boolean APP_MASTER_STOPPED = false;


    /**
     * Master Avro 命令服务器
     */
    private final KafkaYarnAvroServer server;


    /**
     * Kafka Conf
     */
    private final KafkaConfiguration conf;


    /**
     * AppMaster Task ID
     */
    protected final ApplicationId appID;


    /**
     * ResourcesManager Client
     */
    private final AMRMClientAsync<AMRMClient.ContainerRequest> client;


    /**
     * Avro 协议
     */
    private final KafkaYarnMasterProtocol protocol;


    /**
     * Kafka Client
     */
    protected final KafkaAMRMClient kafkaAMRMClient;


    /**
     * App Wars
     */
    protected final String kafkaConf;


    /**
     * 内部队列消息
     */
    private final BlockingQueue<Object> launcherQueue = new LinkedBlockingQueue<Object>();


    /**
     * 构造方法
     * @param conf Kafka Conf
     * @param appAttemptID AppMaster Task ID
     */
    KafkaAppMaster(KafkaConfiguration conf, String kafkaConf, ApplicationId appAttemptID) {
        this(conf, new YarnConfiguration(), kafkaConf, appAttemptID);
    }


    /**
     * 构造方法
     * @param conf Kafka Conf
     * @param hadoopConf Hadoop Conf
     * @param appID AppMaster Task ID
     */
    KafkaAppMaster(KafkaConfiguration conf,
                   Configuration hadoopConf, String kafkaConf, ApplicationId appID) {
        this.conf = conf;
        this.kafkaConf = kafkaConf;
        this.appID = appID;

        int pri = conf.getInt(KafkaOnYarn.MASTER_CONTAINER_PRIORITY, 0);
        this.DEFAULT_PRIORITY.setPriority(pri);

        int heartBeatIntervalMs = conf.getInt(KafkaOnYarn.MASTER_HEARTBEAT_INTERVAL_MILLIS, 10000);


        this.kafkaAMRMClient = new KafkaAMRMClient(appID, conf, hadoopConf);

        this.client = AMRMClientAsync.createAMRMClientAsync(heartBeatIntervalMs, this);
        this.client.init(hadoopConf);
        this.client.start();


        this.protocol = new KafkaYarnMasterProtocol(conf, kafkaAMRMClient);
        this.server = new KafkaYarnAvroServer(KafkaYarnProtocol.class, protocol);
        int port = conf.getInt(KafkaOnYarn.MASTER_AVRO_PORT, 4880);

        SocketPortFactory portFactory = new FreeSocketPortFactory();
        port = portFactory.getSocketPort(port, conf.getInt(KafkaOnYarn.MASTER_AVRO_PORT + ".step", 1));
        this.server.setHost(NetworkUtils.getLocalhostName());
        this.server.setPort(port);

        LOG.info("kafka application master start at: " + getAvroServerHost() + ":" + getAvroServerPort());

        //把使用的端口重新设置, 这样是防止在同一个 Node 上运行多个 AppMaster
        conf.setProperty(KafkaOnYarn.MASTER_AVRO_PORT, port);

        protocol.setAppMaster(this);
    }


    public void initStartDefaultContainer(int num) throws AvroRemoteException {
        int mb = Integer.parseInt(System.getProperty("kafka.instance.memory", "512"));
        int cores = Integer.parseInt(System.getProperty("kafka.instance.cores", "1"));
        for (int i = 0; i < num; i++) {
            this.protocol.addInstance(mb, cores, null);
        }
    }



    public void initAndStartLauncher() {
        Thread thread = new Thread(this);
        thread.setDaemon(true);
        thread.setName("kafka-app-master-worker-thread");
        thread.start();
    }




    private void startAppMasterUI(final Map<String, String> argsMap) {
        Thread uiThread = new Thread(new Runnable() {
            @Override
            public void run() {
                List<String> argList = new ArrayList<String>();
                for (Map.Entry<String, String> entry : argsMap.entrySet()) {
                    argList.add("--" + entry.getKey());
                    argList.add(entry.getValue());
                }

                LOG.info("start ui... args: " + argList.toString());
                Method main = null;
                try {
                    main = Class.forName("com.quantifind.kafka.offsetapp.OffsetGetterWeb").getMethod("main", String[].class);
                    main.invoke(null, new Object[][]{ argList.toArray(new String[argList.size()]) });
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }

            }
        });
        uiThread.setDaemon(true);
        uiThread.setName("kafka-app-master-ui-thread");
        uiThread.start();
    }



    public void registerAppMaster() throws Exception {
        final String host = getAvroServerHost();
        final int port = getAvroServerPort();
        InetAddress inetAddress = InetAddress.getByName(host);

        LOG.info("registration rm, app host name: " + inetAddress + ":" + port);

        RegisterApplicationMasterResponse resp =
                client.registerApplicationMaster(host, port, null);
        LOG.info("Got a registration response " + resp);
        LOG.info("Max Capability " + resp.getMaximumResourceCapability());
    }


    public void serve() {
        this.server.serve();
    }


    public void stop() {
        if(!APP_MASTER_STOPPED) {
            APP_MASTER_STOPPED = true;
            LOG.info("Stop Kafka Instances");
            try {
                protocol.stopAll();
            } catch (AvroRemoteException e) {
                LOG.warn("", e);
            }

            LOG.info("Stop Kafka App Master Avro Server");
            if (server != null) {
                try {
                    server.stop();
                } catch (Exception e) {
                    LOG.warn("", e);
                }
            }
        }
    }


    public ApplicationId getAppID() {
        return appID;
    }


    public String getKafkaConf() {
        return kafkaConf;
    }

    public void add(Object o) {
        launcherQueue.add(o);
    }

    @Override
    public void run() {
        while (!APP_MASTER_STOPPED && client.getServiceState() == Service.STATE.STARTED &&
                !Thread.currentThread().isInterrupted()) {
            LOG.info("kafka-app-master-worker-thread start.");
            try {
                Object take = launcherQueue.take();
                if(take instanceof Shutdown) {
                    LOG.warn("shutdown message, app master stopping.");
                    Thread.sleep(3000);
                    this.stop();
                    break;
                } else if(take instanceof Container) {
                    Container container = (Container) take;
                    LOG.info("LAUNCHER: Taking container with id (" + container.getId() + ") from the queue.");
                }
            } catch (InterruptedException e) {
                LOG.warn(e.getMessage());
                if (client.getServiceState() == Service.STATE.STARTED) {
                    LOG.error("Launcher thread interrupted : ", e);
                }
                break;
            } catch (Exception e) {
                LOG.error("Launcher thread I/O exception : ", e);
            }
        }

        LOG.warn("kafka-app-master-worker-thread stoped.");
    }




    public synchronized void newContainer(int memory, int cores) {
        Resource resource = Resource.newInstance(memory, cores);
        AMRMClient.ContainerRequest req = new AMRMClient.ContainerRequest(
                resource,
                null, // String[] nodes,
                null, // String[] racks,
                DEFAULT_PRIORITY);

        LOG.info("开始准备申请 Container: " + req.toString());

        this.client.addContainerRequest(req);
    }


    @Override
    public void onContainersCompleted(List<ContainerStatus> statuses) {
        LOG.info("HB: Containers completed (" + statuses.size() + "), so releasing them.");

        for (ContainerStatus status : statuses) {
            LOG.info(status.toString());
            this.kafkaAMRMClient.releaseContainer(status);

            if(status.getExitStatus() > 0 && kafkaAMRMClient.getRunningContainer() == 0) {
                stop();
            }
        }
    }

    @Override
    public void onContainersAllocated(List<Container> containers) {
        LOG.info("已申请到 Container: " + containers.size());
        LOG.info(containers.toString());

        this.kafkaAMRMClient.addAllocatedContainers(containers);
    }

    @Override
    public void onShutdownRequest() {
        LOG.info("onShutdownRequest(): kafka app master shutdown");
        stop();
    }

    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {
        LOG.info("updatedNodes(): " + updatedNodes);
    }

    @Override
    public float getProgress() {
        //ApplicationMaster 进度一直是 50%
        return 0.5f;
    }

    @Override
    public void onError(Throwable e) {
        LOG.warn("error print: ");
        LOG.error(ExceptionUtils.getFullStackTrace(e));
    }


    public void finishedAppMaster() throws Exception {
        try {
            client.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
                    "AllDone", null);
        } catch (Exception e) {
            LOG.error(ExceptionUtils.getFullStackTrace(e));
        }
    }



    public void failedApp() throws Exception {
        try {
            client.unregisterApplicationMaster(FinalApplicationStatus.FAILED,
                    "FAILED", null);
        } catch (Exception e) {
            LOG.error(ExceptionUtils.getFullStackTrace(e));
        }
    }

    public String getAvroServerHost() {
        return this.server.getHost();
    }

    public int getAvroServerPort() {
        return this.server.getPort();
    }



    public static void main(String[] args) throws Exception {
        LOG.info("Starting the AM!!!!");

        Options opts = new Options();
        opts.addOption("app_attempt_id", true, "App Attempt ID. Not to be used unless for testing purposes");
        opts.addOption("num", true, "default Kafka Instance Number, default 1.");


        CommandLine cl = new GnuParser().parse(opts, args);

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
            LOG.error("appAttemptID is not specified for storm master");
            throw new Exception("appAttemptID is not specified for storm master");
        }

        int num = 1;
        if (cl.hasOption("num")) {
            String n = cl.getOptionValue("num", "1");
            num = Integer.parseInt(n);

            if(num < 1) {
                num = 1;
            }
        }
        KafkaConfiguration conf = new KafkaConfiguration("kafka.yarn.properties");

        Iterator keys = conf.getKeys();
        Map<String, String> argsMap = new HashedMap();
        while (keys.hasNext()) {
            String key = keys.next().toString();
            if(key.startsWith("yarn.kafka.appmaster.args")) {
                argsMap.put(key.substring("yarn.kafka.appmaster.args.".length()), conf.getString(key));
            }
        }

        YarnConfiguration hadoopConf = new YarnConfiguration();

        KafkaAppMaster server = new KafkaAppMaster(conf, hadoopConf, "", appAttemptID.getApplicationId());
        try {
            server.startAppMasterUI(argsMap);
            server.registerAppMaster();

            LOG.info("Starting launcher");
            server.initAndStartLauncher();

            LOG.info("Starting Kafka Container");
            //server.initStartDefaultContainer(num);

            LOG.info("Starting Kafka App Master Avro Server");
            server.serve();

            LOG.info("Kafka AMRMClient::unregisterApplicationMaster, [success]");
            server.finishedAppMaster();
        } catch (Exception e) {
            LOG.info("Kafka AMRMClient::unregisterApplicationMaster, [failed]");
            server.failedApp();
            LOG.warn(ExceptionUtils.getFullStackTrace(e));
        } finally {
            LOG.info("Stop Kafka Application Master.");
            server.stop();

        }
        System.exit(0);
    }


}
