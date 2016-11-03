package azkaban.user;

/**
 * @author lcs
 * @class UserRole
 * @date 2016/9/30.
 * @describe 用户-角色
 */
public class UserRole {
    private String userName;
    private String roleName;

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getRoleName() {
        return roleName;
    }

    public void setRoleName(String roleName) {
        this.roleName = roleName;
    }
}
