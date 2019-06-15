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
public class Master implements Watcher {
    private static Logger logger = LoggerFactory.getLogger(Master.class);
    String hostPort;
    ZooKeeper zk;
    String serverId = Long.toString(new Random().nextLong());
    Boolean isLeader;
    private static final String path = "/master";

    public Master(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZk() throws IOException {
        zk = new ZooKeeper(hostPort,15*1000,this);
    }

    void stopZk() throws InterruptedException {
        zk.close();
    }

    Boolean checkMaster(){
        while (true){
            Stat stat = new Stat();
            try {
                byte[] data = zk.getData(path, false, stat);
                isLeader = new String(data).equals(serverId);

                return true;
            } catch (KeeperException e) {
                e.printStackTrace();
                return false;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    void runForMaster(){
        while(true){
            try {
                zk.create(path,serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);
                isLeader = true;
                break;
            } catch (KeeperException e) {
                e.printStackTrace();
                isLeader = false;
                break;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if(checkMaster()) break;
        }
    }

     AsyncCallback.DataCallback dataCallback = (i, s, o, bytes, stat) -> {
         switch (KeeperException.Code.get(i)){
             case CONNECTIONLOSS:
                 checkMasterAsync();
                 return;
             case NONODE:
                 runForMasterAsync();
                 return;
         }
     };

    void checkMasterAsync(){
        zk.getData(path, false, dataCallback,null);
    }

    AsyncCallback.StringCallback callback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int i, String s, Object o, String s1) {
            switch (KeeperException.Code.get(i)){
                case CONNECTIONLOSS:
                    checkMasterAsync();
                    return;
                case OK:
                    isLeader = true;
                    break;
                default:
                    isLeader = false;
            }
            logger.info("i am "+(isLeader?"":"not")+" leader");
        }
    };

    void runForMasterAsync(){
        zk.create(path, serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,callback ,null);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        logger.info(watchedEvent.toString());
    }


    AsyncCallback.StringCallback parentCallback = (i, s, o, s1) -> {
        switch (KeeperException.Code.get(i)){
            case CONNECTIONLOSS:
                createParent(s, (byte[]) o);
            case OK:
                logger.info("Parent created");
                break;
            case NODEEXISTS:
                logger.info("parent already registered :" + s);
                break;
            default:
                logger.info("some went wrong: "+KeeperException.create(KeeperException.Code.get(i),s));
        }
    };

    void bootstrap(){
        createParent("/workers",new byte[0]);
        createParent("/assign",new byte[0]);
        createParent("/tasks",new byte[0]);
        createParent("/status",new byte[0]);
    }

    void createParent(String path,byte[]data){
        zk.create(path,data, ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT,parentCallback,data);
    }

    public static void main(String[]args) throws IOException, InterruptedException {
        Master master = new Master("127.0.0.1:2181");
        master.startZk();
//        master.runForMaster();
//        if(master.isLeader){
//            logger.info("i am the leader");
//            Thread.sleep(60000);
//        }else{
//            logger.info("someone else is the leader");
//        }

        master.runForMasterAsync();

        master.stopZk();
    }
}
