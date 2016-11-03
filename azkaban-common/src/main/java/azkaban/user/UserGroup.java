package azkaban.user;

/**
 * @author lcs
 * @class UserGroup
 * @date 2016/9/30.
 * @describe 用户-组
 */
public class UserGroup {
    private String userName;
    private String groupName;

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }
}
