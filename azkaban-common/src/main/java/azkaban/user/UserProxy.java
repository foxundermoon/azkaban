package azkaban.user;

/**
 * @author lcs
 * @class UserProxy
 * @date 2016/9/30.
 * @describe 用户-代理用户
 */
public class UserProxy {
    private String userName;
    private String proxyName;

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getProxyName() {
        return proxyName;
    }

    public void setProxyName(String proxyName) {
        this.proxyName = proxyName;
    }
}
