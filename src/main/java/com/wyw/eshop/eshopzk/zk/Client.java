package com.wyw.eshop.eshopzk.zk;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Client implements Watcher {
    private Logger logger = LoggerFactory.getLogger(Client.class);
    ZooKeeper zk;
    String hostPort;

    public Client(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZk(){

    }

    @Override
    public void process(WatchedEvent watchedEvent) {

    }
}
