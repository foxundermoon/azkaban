package azkaban.utils;

import azkaban.trigger.TriggerManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import javax.mail.MessagingException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Zookeeper 实现主备节点自动切换功能
 */
public class ElectLeader implements Watcher {
    private static final Logger logger = Logger.getLogger(ElectLeader.class);
    private static final String AZKABAN_ZOOKEEPER_CONNECTION_ADDRESS = "azkaban.zookeeper.address";
    private static final String AZKABAN_ZOOKEEPER_NODE_NAME = "azkaban.zookeeper.node.name";
    private static final String AZKABAN_NODE_IP = "azkaban.node.ip";
    private static final String AZKABAN_CONNECTION_ZOOKEEPER_ERROR_EMAILS = "azkaban.connection.zookeeper.error.emails";
    private static final String AZKABAN_ZOOKEEPER_SESSION_TIMEOUT = "azkaban.zookeeper.session.timeout";
    private static final String AZKABAN_IS_MASTER_NODE = "azkaban.is.master.node";
    private static int session_timeout = 5000;
    private static String connection_address = "192.168.10.84:2181";
    private static String znode_name = "/azkaban";
    private ZooKeeper zk;
    private static String serverId = "192.168.10.85";
    private volatile boolean connected = false;
    private volatile boolean expired = false;
    enum MasterStates {RUNNING, ELECTED, NOTELECTED}

    ;
    private volatile MasterStates state = MasterStates.RUNNING;

    MasterStates getState() {
        return state;
    }

    private static TriggerManager triggerManager;
    private static AbstractMailer mailer;
    private static List<String> emailList = new ArrayList<String>();
    private static boolean is_master_node=false;//是否是主节点

    public void startZk() throws IOException {
        zk = new ZooKeeper(connection_address, session_timeout, this);
    }

    public void stopZk() {
        if (zk != null) {
            try {
                zk.close();
            } catch (InterruptedException e) {
                logger.error("(ElectLeader)Interrupted while closing ZooKeeper session.", e);
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
                    TriggerManager.isMaster = false;//关闭开关，执行TriggerManager的TriggerScannerThread线程
                    return;
                case OK:
                    state = MasterStates.ELECTED;
                    TriggerManager.isMaster = true;//开启开关，执行TriggerManager的TriggerScannerThread线程
                    ElectLeader.triggerManager.loadTriggers();//加载trigger到triggers队列和map中
                    //如果当前节点为是backup，则当该节点成为leader的时候，启动一个监控:监控临时目录是否被删除
                    if(!is_master_node ){
                        checkData();
                    }

                    break;
                case NODEEXISTS:
                    state = MasterStates.NOTELECTED;
                    TriggerManager.isMaster = false;//关闭开关，执行TriggerManager的TriggerScannerThread线程
                    ElectLeader.triggerManager.loadTriggers();//加载trigger到map中，不加载到triggers队列中
                    // 添加Watcher
                    addMasterWatcher();
                    //如果是master，则将zookeeper上的临时目录删除
                    if(is_master_node){
                        try {
                            logger.error("(ElectLeader) delete zookeeper path");
                            zk.delete(znode_name,0);
                        } catch (Exception e) {
                            logger.error("(ElectLeader) delete zookeeper path error",e);
                        }
                    }
                    break;
                default:
                    state = MasterStates.NOTELECTED;
                    TriggerManager.isMaster = false;//关闭开关，执行TriggerManager的TriggerScannerThread线程
                    logger.error("(ElectLeader)Something went wrong when running for master.",
                            KeeperException.create(KeeperException.Code.get(rc), path));
            }
            logger.info("(ElectLeader)I'm " + (state == MasterStates.ELECTED ? "" : "not ") + "the leader " + serverId);
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
                    logger.error("(ElectLeader)Error when reading data.", KeeperException.create(KeeperException.Code.get(rc), path));
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
            if (event.getType() == Event.EventType.NodeDeleted) {
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
                    logger.error("(ElectLeader)It sounds like the previous master is gone, " +
                            "so let's run for master again.");
                    break;
                default:
                    checkMaster();
                    break;
            }
        }
    };

    public static void main(String[] args) throws InterruptedException, IOException {
        ElectLeader m = new ElectLeader();
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
        logger.info("(ElectLeader) Processing event: " + e.toString());

        if (e.getType() == Event.EventType.None) {
            switch (e.getState()) {
                case SyncConnected:
                    connected = true;
                case Disconnected:
                    connected = false;
                    logger.error("(ElectLeader) Azkaban connect zookeeper error");
                    sendEmail("Azkaban connect zookeeper exception", "text/html", "<h2 style=\"color:#FF0000\">" + serverId + " azkaban connect zookeeper exception!</h2>", emailList);
                    break;
                case Expired:
                    expired = true;
                    connected = false;
                    logger.error("(ElectLeader) Session expiration");
                default:
                    break;
            }
        }else if(e.getType() == Event.EventType.NodeDeleted){//临时目录被删除，设置expired = true，并停止调度任务
            logger.error("(ElectLeader) the zookeeper path has deleted");
            expired = true;
            connected = false;
            TriggerManager.isMaster = false;//关闭开关，执行TriggerManager的TriggerScannerThread线程
        }

    }

    /**
     * 发送告警邮件
     *
     * @param subject
     * @param mimetype
     * @param message
     * @param emailList
     * @throws Exception
     */
    public void sendEmail(String subject, String mimetype, String message, List<String> emailList) {

        EmailMessage email =
                mailer.prepareEmailMessage(subject, mimetype, emailList);
        email.setBody(message);
        email.setTLS("false");
        try {
            email.sendEmail();
        } catch (MessagingException e) {
            logger.error("(ElectLeader) send email error", e);
        }
    }

    /**
     * 向zookeeper注册节点
     */
    public static void zkStart(Props props, TriggerManager triggerManager) {
        logger.info("(ElectLeader) ---zkStart---");
        ElectLeader.triggerManager = triggerManager;
        connection_address = props.getString(AZKABAN_ZOOKEEPER_CONNECTION_ADDRESS);
        znode_name = props.getString(AZKABAN_ZOOKEEPER_NODE_NAME);
        serverId = props.getString(AZKABAN_NODE_IP);
        session_timeout = props.getInt(AZKABAN_ZOOKEEPER_SESSION_TIMEOUT);
        is_master_node= props.getBoolean(AZKABAN_IS_MASTER_NODE);
        mailer = new AbstractMailer(props);//初始化mailer对象
        String[] emails = props.getString(AZKABAN_CONNECTION_ZOOKEEPER_ERROR_EMAILS).split(",");//连接zk异常报警邮件
        for (String email : emails) {
            emailList.add(email);
        }
        Thread enrollToZookeeper = new Thread(new Runnable() {
            @Override
            public void run() {
                ElectLeader electLeader = null;
                while (true) {
                    electLeader = new ElectLeader();
                    try {
                        electLeader.startZk();
                    } catch (IOException e) {
                        logger.error("(ElectLeader) start zookeeper error", e);
                    }
                    while (!electLeader.isConnected()) {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            logger.error("(ElectLeader) thread sleep error", e);
                        }
                    }

                    electLeader.enroll();
                    while (!electLeader.isExpired()) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            logger.error("(ElectLeader) thread sleep error", e);
                        }
                    }

                    electLeader.stopZk();
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        logger.error("(ElectLeader) thread sleep error", e);
                    }
                }
            }
        }, "enrollToZookeeper");
        enrollToZookeeper.setDaemon(true);
        enrollToZookeeper.start();
    }


    public void checkData() {
        zk.getData(znode_name, true, masterCheckCallBack2, null);
    }
    AsyncCallback.DataCallback masterCheckCallBack2 = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            if (!serverId.equals(new String(data))) {
                logger.error("(ElectLeader) the zookeeper path="+znode_name+"，data has changed ,data="+new String(data));
            }

        }
    };

}

