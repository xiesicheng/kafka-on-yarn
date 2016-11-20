package com.ivyft.kafka.yarn.protocol;

import com.ivyft.kafka.yarn.KafkaAMRMClient;
import com.ivyft.kafka.yarn.KafkaAppMaster;
import com.ivyft.kafka.yarn.KafkaConfiguration;
import com.ivyft.kafka.yarn.Shutdown;
import org.apache.avro.AvroRemoteException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/11/27
 * Time: 20:59
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class KafkaYarnMasterProtocol implements KafkaYarnProtocol {


    protected final KafkaConfiguration conf;


    protected final KafkaAMRMClient amrmClient;



    protected KafkaAppMaster appMaster;


    private static Logger LOG = LoggerFactory.getLogger(KafkaYarnMasterProtocol.class);



    public KafkaYarnMasterProtocol(KafkaConfiguration conf, KafkaAMRMClient client) {
        this.conf = conf;
        this.amrmClient = client;
    }

    @Override
    public Void shutdown() throws AvroRemoteException {
        LOG.info("remote shutdown...");
        if(appMaster != null) {
            LOG.info("remote shutdown message...");
            appMaster.add(new Shutdown());
        }
        return null;
    }

    @Override
    public List<NodeContainer> listInstance() throws AvroRemoteException {
        return this.amrmClient.listKafkaNodes();
    }

    @Override
    public Void addInstance(int memory, int cores, int brokerId) throws AvroRemoteException {
        if(amrmClient.containerIsEmpty()) {
            this.appMaster.newContainer(memory, cores);
        }
        this.amrmClient.startInstance(brokerId, appMaster.getKafkaConf());
        return null;
    }

    @Override
    public Void stopInstance(CharSequence id, IdType idType) throws AvroRemoteException {
        if(idType == IdType.CONTAINER_ID) {
            try {
                this.amrmClient.stopInstance(id.toString());
                return null;
            } catch (Exception e) {
                throw new AvroRemoteException(e);
            }
        } else {
            try {
                List<NodeContainer> nodeContainers = listInstance();
                for (NodeContainer nodeContainer : nodeContainers) {
                    if (StringUtils.equals(String.valueOf(id), String.valueOf(nodeContainer.getNodeHost()))) {
                        this.amrmClient.stopInstance(nodeContainer.getContainerId().toString());
                    }
                }
            } catch (Exception e) {
                throw new AvroRemoteException(e);
            }

            return null;
        }
    }

    @Override
    public Void stopAll() throws AvroRemoteException {
        List<NodeContainer> nodeContainers = listInstance();
        for (NodeContainer container : nodeContainers) {
            try {
                this.amrmClient.stopInstance(container);
            } catch (Exception e) {
                throw new AvroRemoteException(e);
            }
        }
        return null;
    }

    public Void close() throws AvroRemoteException {
        return null;
    }


    public KafkaConfiguration getConf() {
        return conf;
    }

    public KafkaAMRMClient getAmrmClient() {
        return amrmClient;
    }

    public KafkaAppMaster getAppMaster() {
        return appMaster;
    }

    public void setAppMaster(KafkaAppMaster appMaster) {
        this.appMaster = appMaster;
    }
}
