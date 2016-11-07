package azkaban.utils;

import azkaban.trigger.TriggerManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import javax.mail.MessagingException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Zookeeper 实现主备节点自动切换功能
 */
public class ElectLeader implements Watcher {
    private static final Logger LOG = Logger.getLogger(ElectLeader.class);
    private static final String AZKABAN_ZOOKEEPER_CONNECTION_ADDRESS="azkaban.zookeeper.address";
    private static final String AZKABAN_ZOOKEEPER_NODE_NAME="azkaban.zookeeper.node.name";
    private static final String AZKABAN_NODE_IP="azkaban.node.ip";
    private static final String AZKABAN_CONNECTION_ZOOKEEPER_ERROR_EMAILS="azkaban.connection.zookeeper.error.emails";
    private static final String AZKABAN_CONNECTION_ZOOKEEPER_MAX_TIME="azkaban.connection.zookeeper.max.time";
    private static final int SESSION_TIMEOUT = 5000;
    private static  String connection_address = "192.168.10.84:2181";
    private static  String znode_name = "/azkaban";
    private Random random = new Random(System.currentTimeMillis());
    private ZooKeeper zk;
    private static String serverId = "192.168.10.85";
    private volatile boolean connected = false;
    private volatile boolean expired = false;
    enum MasterStates {RUNNING, ELECTED, NOTELECTED};
    private volatile MasterStates state = MasterStates.RUNNING;
    MasterStates getState() {
        return state;
    }
    private static TriggerManager triggerManager;
    private static AbstractMailer mailer;
    private static boolean isStop=false;
    private static List<String> emailList=new ArrayList<String>();
    public void startZk() throws IOException {
        zk = new ZooKeeper(connection_address, SESSION_TIMEOUT, this);
    }

    public void stopZk() {
        if (zk != null) {
            try {
                zk.close();
            } catch (InterruptedException e) {
                LOG.warn("(ElectLeader)Interrupted while closing ZooKeeper session.", e);
            }
        }
    }

    /**
     * 抢注节点
     */
    public void enroll() {
        zk.create(znode_name,
                serverId.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                masterCreateCallBack, null);
    }

    AsyncCallback.StringCallback masterCreateCallBack = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS://连接异常
                    //网络问题，需要检查节点是否创建成功
                    checkMaster();
                    Switch.isMaster=false;//关闭开关，执行TriggerManager的TriggerScannerThread线程
                    return;
                case OK:
                    System.out.println("---------I'm the leader--------- ");
                    LOG.info("(ElectLeader) I'm the leader");
                    state = MasterStates.ELECTED;
                    ElectLeader.triggerManager.loadTriggers();//加载triggers
                    Switch.isMaster=true;//开启开关，执行TriggerManager的TriggerScannerThread线程
                    break;
                case NODEEXISTS:
                    state = MasterStates.NOTELECTED;
                    Switch.isMaster=false;//关闭开关，执行TriggerManager的TriggerScannerThread线程
                    // 添加Watcher
                    addMasterWatcher();
                    break;
                default:
                    state = MasterStates.NOTELECTED;
                    Switch.isMaster=false;//关闭开关，执行TriggerManager的TriggerScannerThread线程
                    LOG.error("(ElectLeader)Something went wrong when running for master.",
                            KeeperException.create(KeeperException.Code.get(rc), path));
            }
            LOG.info("(ElectLeader)I'm " + (state == MasterStates.ELECTED ? "" : "not ") + "the leader " + serverId);
        }
    };

    public void checkMaster() {
        zk.getData(znode_name, false, masterCheckCallBack, null);
    }

    AsyncCallback.DataCallback masterCheckCallBack = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkMaster();
                    return;
                case NONODE:
                    // 节点未创建，再次注册
                    enroll();
                    return;
                case OK:
                    if (serverId.equals(new String(data))) {
                        state = MasterStates.ELECTED;
                    } else {
                        state = MasterStates.NOTELECTED;
                        addMasterWatcher();
                    }
                    break;
                default:
                    LOG.error("(ElectLeader)Error when reading data.",KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    void addMasterWatcher() {
        zk.exists(znode_name,
                masterExistsWatcher,
                masterExistsCallback,
                null);
    }
    Watcher masterExistsWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            if(event.getType() == Event.EventType.NodeDeleted){
                assert znode_name.equals(event.getPath());
                enroll();
            }
        }
    };
    AsyncCallback.StatCallback masterExistsCallback = new AsyncCallback.StatCallback() {
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    addMasterWatcher();
                    break;
                case OK:
                    break;
                case NONODE:
                    state = MasterStates.RUNNING;
                    enroll();
                    LOG.info("(ElectLeader)It sounds like the previous master is gone, " +
                            "so let's run for master again.");
                    break;
                default:
                    checkMaster();
                    break;
            }
        }
    };

    public static void main(String[] args) throws InterruptedException, IOException {
        ElectLeader m = new ElectLeader( );
        m.startZk();

        while (!m.isConnected()) {
            Thread.sleep(100);
        }
        m.enroll();
        while (!m.isExpired()) {
            Thread.sleep(1000);
        }

        m.stopZk();
    }
    boolean isConnected() {
        return connected;
    }

    boolean isExpired() {
        return expired;
    }

    @Override
    public void process(WatchedEvent e) {
        LOG.info("(ElectLeader) Processing event: " + e.toString());
        if (e.getType() == Event.EventType.None) {
            switch (e.getState()) {
                case SyncConnected:
                    connected = true;
                    break;
                case Disconnected:
                    connected = false;
                    break;
                case Expired:
                    expired = true;
                    connected = false;
                    LOG.error("(ElectLeader) Session expiration");
                default:
                    break;
            }
        }
    }

    /**
     * 发送告警邮件
     * @param subject
     * @param mimetype
     * @param message
     * @param emailList
     * @throws Exception
     */
    public void sendEmail(String subject,String mimetype,String message, List<String> emailList) {

        EmailMessage email =
                mailer.prepareEmailMessage(subject, mimetype, emailList);
        email.setBody(message);
        email.setTLS("false");
        try {
            email.sendEmail();
        } catch (MessagingException e) {
            LOG.error("(ElectLeader) send email error",e);
        }
    }
    /**
     * 向zookeeper注册节点
     */
    public static void zkStart( Props props,TriggerManager triggerManager){
        LOG.info("-------------------zkStart----------------------");
        System.out.println("-------------------zkStart----------------------");

        ElectLeader.triggerManager=triggerManager;
        connection_address=props.getString(AZKABAN_ZOOKEEPER_CONNECTION_ADDRESS);
        znode_name=props.getString(AZKABAN_ZOOKEEPER_NODE_NAME);
        serverId=props.getString(AZKABAN_NODE_IP);
        final long maxTime=props.getLong(AZKABAN_CONNECTION_ZOOKEEPER_MAX_TIME);//连接时长
        mailer = new AbstractMailer(props);//初始化mailer对象
        String[] emails=props.getString(AZKABAN_CONNECTION_ZOOKEEPER_ERROR_EMAILS).split(",");//连接zk异常报警邮件
        for(String email:emails){
            emailList.add(email);
        }
        Thread enrollToZookeeper = new Thread(new Runnable() {
            @Override
            public void run() {
                ElectLeader electLeader = new ElectLeader( );
                while(true){
                    try {
                        electLeader.startZk();
                    } catch (IOException e) {
                        LOG.error("(ElectLeader) start zookeeper error",e);
                    }
                    long firstTime=System.currentTimeMillis();
                    while (!electLeader.isConnected()) {
                        long currentTime=System.currentTimeMillis();
                        long longTime=currentTime-firstTime;
                        if(longTime>maxTime && !isStop){
                            System.out.println("---------------connect zookeeper exception，send mail--------------");
                            LOG.error("(ElectLeader) Azkaban connect zookeeper error");
                            electLeader.sendEmail("Azkaban connect zookeeper exception","text/html","<h2 style=\"color:#FF0000\">"+serverId+" azkaban connect zookeeper exception!</h2>",emailList);
                            isStop=true;
                            break;
                        }
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            LOG.error("(ElectLeader) thread sleep error",e);
                        }
                    }
                    if(electLeader.isConnected()){
                        isStop=false;
                    }
                    if(!isStop){
                        electLeader.enroll();
                        while (!electLeader.isExpired()) {
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException e) {
                                LOG.error("(ElectLeader) thread sleep error",e);
                            }
                        }
                    }

                    electLeader.stopZk();
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        LOG.error("(ElectLeader) thread sleep error",e);
                    }
                }
            }
        }, "enrollToZookeeper");
        enrollToZookeeper.setDaemon(true);
        enrollToZookeeper.start();
    }



}
