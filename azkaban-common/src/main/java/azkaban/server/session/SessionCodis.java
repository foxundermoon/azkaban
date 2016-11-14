package azkaban.server.session;

import azkaban.user.User;
import azkaban.user.UserManager;
import azkaban.user.UserManagerException;
import azkaban.utils.Props;
import com.wandoulabs.jodis.JedisResourcePool;
import com.wandoulabs.jodis.RoundRobinJedisPool;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;

import javax.servlet.ServletException;
import java.util.HashMap;

/**
 * @author lcs
 * @class SessionCodis
 * @date 2016/10/31.
 * @describe 将session信息存取到redis中
 */
public class SessionCodis {
    private static Logger logger = Logger.getLogger(SessionCodis.class);
    private static final String AZKABAN_ZOOKEEPER_ADDRESS = "azkaban.zookeeper.address";
    private static final String AZKABAN_ZOOKEEPER_PROXY_DIR = "azkaban.zookeeper.proxy.dir";
    private static final String AZKABAN_JEDISPOOL_MAX_Idle = "azkaban.jedispool.max.Idle";
    private static final String AZKABAN_JEDISPOOL_MAX_TOTAL = "azkaban.jedispool.max.total";
    private static final String AZKABAN_JEDISPOOL_MAX_WAIT = "azkaban.jedispool.max.wait";
    private static final String AZKABAN_SESSION_TIME_OUT = "azkaban.session.time.out";
    private String zkAddr = "192.168.10.85:2181";
    private String zkProxyDir = "/zk/codis/db_bi_test/proxy";
    private int maxTotal = 50;
    private int maxIdle = 30;
    private int maxWait = 1000 * 60;
    private int sessionTimeOut = 86400;//session过期时间
    private UserManager userManager = null;
    private static HashMap<String, JedisResourcePool> jedisPoolMap = new HashMap<String, JedisResourcePool>();

    public SessionCodis(Props props, UserManager userManager) {
        this.zkAddr = props.getString(AZKABAN_ZOOKEEPER_ADDRESS);
        this.zkProxyDir = props.getString(AZKABAN_ZOOKEEPER_PROXY_DIR);
        this.maxTotal = Integer.parseInt(props.getString(AZKABAN_JEDISPOOL_MAX_TOTAL));
        this.maxIdle = Integer.parseInt(props.getString(AZKABAN_JEDISPOOL_MAX_Idle));
        this.maxWait = Integer.parseInt(props.getString(AZKABAN_JEDISPOOL_MAX_WAIT));
        this.sessionTimeOut = Integer.parseInt(props.getString(AZKABAN_SESSION_TIME_OUT));
        this.userManager = userManager;
        getPool();
    }

    /**
     * 获取jedis连接池
     *
     * @return
     */
    public JedisResourcePool getPool() {
        if (!jedisPoolMap.containsKey(zkProxyDir)) {
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(maxTotal);//最大连接数
            poolConfig.setMaxIdle(maxIdle);//最大空闲连接
            poolConfig.setMaxWaitMillis(maxWait);//最大等待时间 当没有可用连接时,连接池等待连接被归还的最大时间(以毫秒计数),超过时间则抛出异常,如果设置为-1表示无限等待
            JedisResourcePool jedisPool = RoundRobinJedisPool.create()
                    .poolConfig(poolConfig)
                    .timeoutMs(50000)
                    .curatorClient(zkAddr, 10000)
                    .zkProxyDir(zkProxyDir).build();
            jedisPoolMap.put(zkProxyDir, jedisPool);
        }
        return jedisPoolMap.get(zkProxyDir);
    }

    /**
     * 将session对象存储到redis中
     *
     * @param session
     */
    public void addSession(Session session) {
        logger.error("(SessionCodis-addSession) username= " + session.getUser().getUserId());
        JedisResourcePool jedisPool = getPool();
        Jedis jedis = jedisPool.getResource();
        jedis.hset(session.getSessionId(), "username", session.getUser().getUserId());
        jedis.hset(session.getSessionId(), "password", session.getUser().getPassword());
        jedis.hset(session.getSessionId(), "ip", session.getIp());
        jedis.expire(session.getSessionId(), sessionTimeOut);//86400秒（24小时）后过期
        jedis.close();
    }

    /**
     * 根据sessionid，从redis中获取session
     *
     * @param sessionId
     * @return
     */
    public Session getSession(String sessionId) {
        JedisResourcePool jedisPool = getPool();
        Jedis jedis = jedisPool.getResource();
        String username = jedis.hget(sessionId, "username");
        String password = jedis.hget(sessionId, "password");
        String ip = jedis.hget(sessionId, "ip");
        logger.info("(SessionCodis-getSession) sessionId= " + sessionId + ",username=" + username + ",ip=" + ip);
        if (StringUtils.isBlank(sessionId) || StringUtils.isBlank(password) || StringUtils.isBlank(ip)) {
            logger.error("(SessionCodis-getSession) param  error,sessionId="+sessionId+",password="+password+",ip"+ip);
            return null;
        }
        Session session = null;
        try {
            session = createSession(sessionId, username, password, ip);
        } catch (Exception e) {
            logger.error("(SessionCodis-getSession) get session object error",e);
        }
        jedis.close();
        return session;
    }

    /**
     * 根据sessionId,从redis中删除
     *
     * @param sessionId
     */
    public void removeSession(String sessionId) {
        logger.error("(SessionCodis-removeSession) removeSession= " + sessionId);
        JedisResourcePool jedisPool = getPool();
        Jedis jedis = jedisPool.getResource();
        if (jedis.exists(sessionId)) {
            jedis.hdel(sessionId, "username", "password", "ip");
        }
        jedis.close();
    }

    /**
     * 创建session对象
     *
     * @param sessionid
     * @param username
     * @param password
     * @param ip
     * @return
     * @throws UserManagerException
     * @throws ServletException
     */
    private Session createSession(String sessionid, String username, String password, String ip)
            throws UserManagerException, ServletException {
        User user = userManager.getUser(username, password);
        Session session = new Session(sessionid, user, ip);
        return session;
    }
}
