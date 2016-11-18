package com.ivyft.kafka.yarn.protocol;

import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/1/6
 * Time: 18:24
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class KafkaYarnAvroServer extends SpecificResponder {




    protected String host = "0.0.0.0";


    protected int port = 7690;


    protected boolean daemon = false;



    protected final KafkaYarnProtocol jettyYarnProtocol;


    private Server server = null;


    private static Logger LOG = LoggerFactory.getLogger(KafkaYarnAvroServer.class);


    public KafkaYarnAvroServer(Class<? extends KafkaYarnProtocol> iface, Object impl) {
        super(iface, impl);
        this.jettyYarnProtocol = (KafkaYarnProtocol)impl;
    }


    public KafkaYarnAvroServer(String iface, Object impl) throws ClassNotFoundException {
        this((Class<? extends KafkaYarnProtocol>) Class.forName(iface), impl);
    }


    public void serve() {
        try {
            InetAddress inetAddress = InetAddress.getByName(host);
            server = new NettyServer(KafkaYarnAvroServer.this, new InetSocketAddress(inetAddress, port));
            server.start();
            LOG.info("start avro nio socket at: " + inetAddress + ":" + port);
            server.join();
        } catch (Exception e) {
            LOG.warn("", e);
        }
    }


    public void stop(){
        if (server != null) {
            try {
                server.close();
            } catch (Exception e) {
                LOG.warn("", e);
            }
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


    public boolean isDaemon() {
        return daemon;
    }

    public void setDaemon(boolean daemon) {
        this.daemon = daemon;
    }

    public KafkaYarnProtocol getJettyYarnProtocol() {
        return jettyYarnProtocol;
    }
}
