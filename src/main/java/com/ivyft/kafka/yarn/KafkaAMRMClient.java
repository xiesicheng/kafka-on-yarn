

package com.ivyft.kafka.yarn;

import com.ivyft.kafka.yarn.protocol.NodeContainer;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;


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
public class KafkaAMRMClient implements NMClientAsync.CallbackHandler {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaAMRMClient.class);

    private final Map<ContainerId, NodeContainer> CONTAINERID_NODE_MAP = new ConcurrentHashMap<ContainerId, NodeContainer>(5);



    private final Map<ContainerId, NodeContainer> RUNNING_CONTAINERID_NODE_MAP = new ConcurrentHashMap<ContainerId, NodeContainer>(5);



    private final Map<ContainerId, NodeContainer> COMPILED_CONTAINERID_NODE_MAP = new ConcurrentHashMap<ContainerId, NodeContainer>(5);


    private final KafkaConfiguration conf;

    private final Configuration hadoopConf;


    private final BlockingQueue<Container> CONTAINER_QUEUE = new LinkedBlockingQueue<Container>(3);


    private Container currentContainer;

    private final ReentrantLock LOCK = new ReentrantLock();

    private ApplicationId appId;

    private NMClientAsync nmClient;

    public KafkaAMRMClient(ApplicationId appId,
                           KafkaConfiguration conf,
                           Configuration hadoopConf) {
        this.appId = appId;
        this.conf = conf;
        this.hadoopConf = hadoopConf;

        // start am nm client
        this.nmClient = NMClientAsync.createNMClientAsync(this);
        this.nmClient.init(hadoopConf);
        this.nmClient.start();


    }

    public synchronized void addAllocatedContainers(List<Container> containers) {
        if (currentContainer != null) {
            containers.remove(currentContainer);
        }
        LOCK.lock();
        try {
            for (Container container : containers) {
                if (CONTAINER_QUEUE.contains(container)) {
                    continue;
                }

                this.CONTAINER_QUEUE.put(container);
            }
        } catch (InterruptedException e) {
            LOG.info("", e);
        } finally {
            LOCK.unlock();
        }
    }



    public int containerSize() {
        return CONTAINER_QUEUE.size();
    }




    public boolean containerIsEmpty() {
        return CONTAINER_QUEUE.isEmpty();
    }




    public void startInstance(int brokerId, String kafkaConf) {
        try {
            currentContainer = CONTAINER_QUEUE.take();
            LOCK.lock();
            try {
                launchKafkaNodeOnContainer(currentContainer, brokerId, kafkaConf);


                ContainerId containerId = currentContainer.getId();
                NodeId nodeId = currentContainer.getNodeId();

                NodeContainer kafkaAndNode = new NodeContainer();
                kafkaAndNode.setContainerId(containerId.toString());
                kafkaAndNode.setNodeHost(nodeId.getHost());
                kafkaAndNode.setNodePort(nodeId.getPort());
                kafkaAndNode.setNodeHttpAddress(currentContainer.getNodeHttpAddress());

                CONTAINERID_NODE_MAP.put(containerId, kafkaAndNode);
            } finally {
                currentContainer = null;
                LOCK.unlock();
            }
        } catch (Exception e) {
            LOG.error("", e);
        }
    }


    public void stopInstance(NodeContainer kafkaAndNode) throws Exception {
        stopInstance(kafkaAndNode.getContainerId().toString());
    }

    public void stopInstance(String id) throws Exception {
        ContainerId containerId = ConverterUtils.toContainerId(id);

        NodeContainer andNode = RUNNING_CONTAINERID_NODE_MAP.get(containerId);
        if(andNode == null) {
            andNode = CONTAINERID_NODE_MAP.get(containerId);
        }

        NodeId nodeId = NodeId.newInstance(andNode.getNodeHost().toString(), andNode.getNodePort());

        ContainerStatus containerStatus = nmClient.getClient().getContainerStatus(containerId, nodeId);
        LOG.info(containerStatus.toString());

        if(containerStatus.getState() != ContainerState.COMPLETE) {
            nmClient.stopContainerAsync(containerId, nodeId);
        }
    }




    public List<NodeContainer> listKafkaNodes() {
        List<NodeContainer> nodes = new ArrayList<NodeContainer>(3);
        Set<Map.Entry<ContainerId, NodeContainer>> entrySet = RUNNING_CONTAINERID_NODE_MAP.entrySet();
        for (Map.Entry<ContainerId, NodeContainer> entry : entrySet) {
            nodes.add(entry.getValue());
        }
        return nodes;
    }



    /**
     * Container 运行结束调用
     * @param status
     */
    public void releaseContainer(ContainerStatus status) {
        ContainerId containerId = status.getContainerId();
        NodeContainer kafkaAndNode = RUNNING_CONTAINERID_NODE_MAP.get(containerId);


        RUNNING_CONTAINERID_NODE_MAP.remove(containerId);
        if(kafkaAndNode == null) {
            kafkaAndNode = CONTAINERID_NODE_MAP.get(containerId);
        }
        LOG.info("release container: " + containerId + "    " + kafkaAndNode);

        if (null != kafkaAndNode) {
            COMPILED_CONTAINERID_NODE_MAP.put(containerId, kafkaAndNode);
        }

    }


    /**
     * 返回当前有几个 Container 正在运行
     * @return
     */
    public int getRunningContainer() {
        return RUNNING_CONTAINERID_NODE_MAP.size();
    }

    @Override
    public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
        LOG.info("onContainerStarted: " + containerId.toString() + "    " + allServiceResponse);

        NodeContainer andNode = CONTAINERID_NODE_MAP.get(containerId);
        if (null != andNode) {
            RUNNING_CONTAINERID_NODE_MAP.put(containerId, andNode);
        }

        LOG.info("started container: " + containerId + "    " + andNode);
    }

    @Override
    public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
        LOG.info("onContainerStatusReceived: " + containerId.toString() + "    " + containerStatus);

        NodeContainer andNode = CONTAINERID_NODE_MAP.get(containerId);
        LOG.info("received container: " + containerId + "    " + andNode);
    }

    @Override
    public void onContainerStopped(ContainerId containerId) {
        LOG.info("onContainerStopped: " + containerId.toString());

        NodeContainer andNode = CONTAINERID_NODE_MAP.get(containerId);
        RUNNING_CONTAINERID_NODE_MAP.remove(containerId);
        if (null != andNode) {
            COMPILED_CONTAINERID_NODE_MAP.put(containerId, andNode);
        }
    }

    @Override
    public void onStartContainerError(ContainerId containerId, Throwable t) {
        LOG.info("onStartContainerError: " + containerId.toString());
        LOG.warn(ExceptionUtils.getFullStackTrace(t));

        NodeContainer andNode = CONTAINERID_NODE_MAP.get(containerId);
        RUNNING_CONTAINERID_NODE_MAP.remove(containerId);
        if (null != andNode) {
            COMPILED_CONTAINERID_NODE_MAP.put(containerId, andNode);
        }
    }

    @Override
    public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
        LOG.info("onGetContainerStatusError: " + containerId.toString());
        LOG.warn(ExceptionUtils.getFullStackTrace(t));
    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable t) {
        LOG.info("onStopContainerError: " + containerId.toString());
        LOG.warn(ExceptionUtils.getFullStackTrace(t));
    }


    /**
     *
     * 启动 Container
     *
     * @param container Yarn Contaier
     * @throws IOException
     */
    public void launchKafkaNodeOnContainer(Container container,
                                           int brokerId,
                                           String kafkaConf)
            throws IOException {
        //Path[] paths = null;
        // create a container launch context
        ContainerLaunchContext launchContext = Records.newRecord(ContainerLaunchContext.class);
        UserGroupInformation user = UserGroupInformation.getCurrentUser();
        try {
            Credentials credentials = user.getCredentials();
            DataOutputBuffer dob = new DataOutputBuffer();
            credentials.writeTokenStorageToStream(dob);
            ByteBuffer securityTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
            launchContext.setTokens(securityTokens);
        } catch (IOException e) {
            LOG.warn("Getting current user info failed when trying to launch the container"
                    + e.getMessage());
        }

        String appHome = Util.getApplicationHomeForId(appId.toString());
        FileSystem fs = FileSystem.get(this.hadoopConf);
        Path dst = new Path(fs.getHomeDirectory(), appHome + Path.SEPARATOR + "AppMaster.jar");
        if(!fs.exists(dst)) {
            String containingJar = KafkaOnYarn.findContainingJar(KafkaOnYarn.class);
            Path src = new Path(containingJar);
            fs.copyFromLocalFile(false, true, src, dst);
            LOG.info("copy jar from: " + src + " to: " + dst);
        }

        // CLC: local resources includes kafka, conf
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();


        localResources.put("AppMaster.jar", Util.newYarnAppResource(fs, dst));


        String kafkaLibPath = conf.getString("yarn.kafka.node.lib.path");
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

        LOG.info("kafka.home=" + zip.toString());


        String vis = conf.getProperty("kafka.zip.visibility", "PUBLIC");
        if (vis.equals("PUBLIC"))
            localResources.put("lib", Util.newYarnAppResource(fs, zip,
                    LocalResourceType.ARCHIVE, LocalResourceVisibility.PUBLIC));
        else if (vis.equals("PRIVATE"))
            localResources.put("lib", Util.newYarnAppResource(fs, zip,
                    LocalResourceType.ARCHIVE, LocalResourceVisibility.PRIVATE));
        else if (vis.equals("APPLICATION"))
            localResources.put("lib", Util.newYarnAppResource(fs, zip,
                    LocalResourceType.ARCHIVE, LocalResourceVisibility.APPLICATION));

        String containerHome = appHome + Path.SEPARATOR + container.getId().getId();

        Path confDst = Util.copyClasspathConf(fs, containerHome);
        localResources.put("conf", Util.newYarnAppResource(fs, confDst,
                LocalResourceType.FILE,
                LocalResourceVisibility.PRIVATE));

        // CLC: env
        Map<String, String> env = new HashMap<String, String>();
        env.put("KAFKA_LOG_DIR", ApplicationConstants.LOG_DIR_EXPANSION_VAR);
        //env.put("appId", new Integer(_appId.getId()).toString());

        Apps.addToEnvironment(env, ApplicationConstants.Environment.CLASSPATH.name(), "./AppMaster.jar");
        Apps.addToEnvironment(env, ApplicationConstants.Environment.CLASSPATH.name(), "./conf");
        Apps.addToEnvironment(env, ApplicationConstants.Environment.CLASSPATH.name(), "./lib/*");


        launchContext.setEnvironment(env);
        launchContext.setLocalResources(localResources);

        List<String> masterArgs = Util.buildNodeCommands(this.conf,
                currentContainer.getResource().getMemory(),
                brokerId,
                kafkaConf);

        LOG.info("luanch: " + StringUtils.join(masterArgs, "  "));

        launchContext.setCommands(masterArgs);

        try {
            LOG.info("Use NMClient to launch kafka node in container. ");
            nmClient.startContainerAsync(container, launchContext);

            String userShortName = user.getShortUserName();
            if (userShortName != null)
                LOG.info("node log: http://" + container.getNodeHttpAddress() + "/node/containerlogs/"
                        + container.getId().toString() + "/" + userShortName + "/");
        } catch (Exception e) {
            LOG.error("Caught an exception while trying to start a container", e);
            throw new IllegalArgumentException(e);
        }
    }
}
