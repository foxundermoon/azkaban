package azkaban.server.session;

import azkaban.user.User;
import azkaban.user.UserManagerException;
import azkaban.utils.Props;
import com.wandoulabs.jodis.JedisResourcePool;
import com.wandoulabs.jodis.RoundRobinJedisPool;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;

import javax.servlet.ServletException;

/**
 * @author lcs
 * @class SessionCodis
 * @date 2016/10/31.
 * @describe 将session信息存取到redis中
 */
public class SessionCodis {
    private static final String AZKABAN_ZOOKEEPER_ADDRESS="azkaban.zookeeper.address";
    private static final String AZKABAN_ZOOKEEPER_PROXY_DIR="azkaban.zookeeper.proxy.dir";
    private static final String AZKABAN_ZOOKEEPER_MAX_TOTAL="azkaban.zookeeper.max.total";
    private String zkAddr = "192.168.10.85:2181";
    private String zkProxyDir ="/zk/codis/db_bi_test/proxy";
    private int maxTotal=50;
    private static JedisResourcePool jedisPool = null;

    public SessionCodis(Props props){
        this.zkAddr=props.getString(AZKABAN_ZOOKEEPER_ADDRESS);
        this.zkProxyDir=props.getString(AZKABAN_ZOOKEEPER_PROXY_DIR);
        this.maxTotal=Integer.parseInt(props.getString(AZKABAN_ZOOKEEPER_MAX_TOTAL));
        getPool();
    }

    /**
     * 获取jedis连接池
     * @return
     */
    public JedisResourcePool getPool(){
        if(jedisPool==null){
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(maxTotal);//最大连接数
            jedisPool  = RoundRobinJedisPool.create()
                    .poolConfig(poolConfig)
                    .timeoutMs(300000)
                    .curatorClient(zkAddr, 10000)
                    .zkProxyDir(zkProxyDir).build();
        }

        return jedisPool;
    }
    /**
     * 将session对象存储到redis中
     * @param session
     */
    public void addSession(Session session) {
        JedisResourcePool jedisPool =getPool();
        Jedis jedis=jedisPool.getResource();
        jedis.hset(session.getSessionId(),"username", session.getUser().getUserId());
        jedis.hset(session.getSessionId(),"password",session.getUser().getPassword());
        jedis.hset(session.getSessionId(),"ip",session.getIp());
        jedis.expire(session.getSessionId().getBytes(),86400);//86400秒（24小时）后过期
        jedis.close();
    }

    /**
     * 根据sessionid，从redis中获取session
     * @param sessionId
     * @return
     */
    public Session getSession(String sessionId) {
        JedisResourcePool jedisPool =getPool();
        Jedis jedis=jedisPool.getResource();
        String username=jedis.hget(sessionId,"username");
        String password=jedis.hget(sessionId,"password");
        String ip=jedis.hget(sessionId,"ip");
        Session session=null;
        try {
            session=createSession(sessionId,username,password,ip);
        } catch (Exception e) {
            throw new RuntimeException("（getSession）获取session对象错误", e);
        }
        jedis.close();
        return session;
    }

    /**
     * 根据sessionId,从redis中删除
     * @param sessionId
     */
    public void removeSession(String sessionId) {
        JedisResourcePool jedisPool =getPool();
        Jedis jedis=jedisPool.getResource();
        if(jedis.exists(sessionId)){
            jedis.hdel(sessionId,"username","password","ip");
        }
        jedis.close();
    }

    /**
     * 创建session对象
     * @param sessionid
     * @param username
     * @param password
     * @param ip
     * @return
     * @throws UserManagerException
     * @throws ServletException
     */
    private Session createSession(String sessionid,String username, String password, String ip)
            throws UserManagerException, ServletException {
        User user=new User(username);
        user.setPassword(password);
        Session session = new Session(sessionid, user, ip);
        return session;
    }
}
