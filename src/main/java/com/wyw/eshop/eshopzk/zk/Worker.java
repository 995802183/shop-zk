package com.wyw.eshop.eshopzk.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

/**
 * @author wyw
 */
public class Worker implements Watcher {
    private Logger logger = LoggerFactory.getLogger(Worker.class);
    String hostPort;
    ZooKeeper zk;
    String status;
    String serverId = Integer.toString(new Random().nextInt());

    public Worker(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZk() throws IOException {
        zk = new ZooKeeper(hostPort,15000,this);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        logger.info(watchedEvent.toString());
    }

    AsyncCallback.StringCallback stringCallback = (i, s, o, s1) -> {
        switch (KeeperException.Code.get(i)){
            case CONNECTIONLOSS:
                register();
                break;
            case OK:
                logger.info("registered success: "+serverId);
                break;
            case NODEEXISTS:
                logger.info("already registered serverId: "+serverId);
                break;
            default:
                logger.info("something went wrong:"+KeeperException.create(KeeperException.Code.get(i),s));
        }
    };

    void register(){
        zk.create("/workers/worker-"+serverId,"Idle".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,stringCallback,null);
    }

    AsyncCallback.StatCallback statCallback = (i, s, o, stat) -> {
        switch (KeeperException.Code.get(i)){
            case CONNECTIONLOSS:
                updateStatus((String) o);
                return ;
        }
    };

    synchronized void updateStatus(String status){
        if(status == this.status){
            zk.setData("/workers/worker-1",status.getBytes(),-1,statCallback,status);
        }
    }

    void setStatus(String status){
        this.status = status;
        updateStatus(status);
    }
    public static void main(String[]args) throws IOException, InterruptedException {
        Worker worker = new Worker("127.0.0.1:2181");
        worker.startZk();
        worker.register();
        Thread.sleep(30000);
    }
}
