package com.ivyft.kafka.yarn.protocol;


import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;


/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/11/27
 * Time: 19:26
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class KafkaYarnClient implements KafkaYarnProtocol {


    private String host = "localhost";

    private int port = 7690;


    private final KafkaYarnProtocol kafkaYarnProtocol;


    private final Transceiver t;


    /**
     * LOG
     */
    private final static Logger LOG = LoggerFactory.getLogger(KafkaYarnClient.class);


    public KafkaYarnClient() {
        this("localhost", 7690);
    }


    public KafkaYarnClient(String host, int port) {
        this.host = host;
        this.port = port;
        try {
            this.t = new NettyTransceiver(new InetSocketAddress(host, port));
            this.kafkaYarnProtocol = SpecificRequestor.getClient(KafkaYarnProtocol.class,
                    new SpecificRequestor(KafkaYarnProtocol.class, t));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }


    @Override
    public List<NodeContainer> listInstance() throws AvroRemoteException {
        return this.kafkaYarnProtocol.listInstance();
    }

    @Override
    public Void addInstance(int memory, int cores, int brokerId) throws AvroRemoteException {
        return this.kafkaYarnProtocol.addInstance(memory, cores, brokerId);
    }

    @Override
    public Void stopInstance(CharSequence id, IdType idType) throws AvroRemoteException {
        return kafkaYarnProtocol.stopInstance(id, idType);
    }

    @Override
    public Void stopAll() throws AvroRemoteException {
        return this.kafkaYarnProtocol.stopAll();
    }

    @Override
    public Void shutdown() throws AvroRemoteException {
        return this.kafkaYarnProtocol.shutdown();
    }


    public Void close() throws AvroRemoteException {
        try {
            this.t.close();
            return null;
        } catch (IOException e) {
            throw  new AvroRemoteException(e);
        }
    }


    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }


    public static void main(String[] args) throws AvroRemoteException {
        KafkaYarnClient client = new KafkaYarnClient("localhost", 4880);
        client.shutdown();
        client.close();
    }

}
